/* Read-only OFP instruments. This adapter never reads an input, calculates a
   dispatch limit, subscribes to telemetry, or writes to a plan. */
(() => {
  'use strict';
  const last = new WeakMap();
  const finite = Number.isFinite;
  const missing = text => '<p class="instrument-caption">' + text + '</p>';
  function paint(target, markup) {
    if (!target || last.get(target) === markup) return;
    target.innerHTML = markup;
    last.set(target, markup);
  }
  // Categorical measurements: rounded bar caps, never an interpolated curve.
  function bars(target, labels, values, digits, id, empty) {
    const available = values.filter(v => finite(v) && v >= 0);
    if (!available.length || (id === 'fuel' && available.every(v => v === 0))) {
      return paint(target, missing(empty));
    }
    const max = Math.max(1, ...available);
    const rows = labels.map((label,i) => {
      const value = values[i], measured = finite(value) && value >= 0;
      const length = measured ? (210 * value / max).toFixed(2) : 0;
      const y = 19 + 27*i;
      return `<text x="0" y="${y}">${label}</text><path d="M108 ${y-4}h210" stroke="var(--efb-line)" stroke-width="7" stroke-linecap="round"/>
        ${measured && value > 0 ? `<rect x="108" y="${y-8}" width="${length}" height="8" rx="4" fill="url(#${id}-fill)"/>` : ''}
        <text x="388" y="${y}" text-anchor="end">${measured ? value.toFixed(digits) : '—'}</text>`;
    });
    paint(target,`<svg viewBox="0 0 390 112" aria-hidden="true" focusable="false"><defs><linearGradient id="${id}-fill" x2="0" y2="1" gradientUnits="objectBoundingBox"><stop stop-color="var(--efb-link)"/><stop offset="1" stop-color="var(--efb-active)"/></linearGradient></defs>${rows.join('')}</svg>`);
  }
  function envelope(target, {profile, tow, cg, gearUpCg, takeoffEnvelope, gearUpEnvelope}) {
    const f=profile?.forwardCgLimits, a=profile?.aftCgLimits;
    if (![f,a].every(edge => Array.isArray(edge) && edge.length >= 2 && edge.every(p => finite(p?.cgIn) && finite(p?.weightLbs)))
        || ![tow,cg,gearUpCg].every(finite) || typeof takeoffEnvelope?.valid !== 'boolean' || typeof gearUpEnvelope?.valid !== 'boolean') {
      return paint(target,missing('Envelope or load data unavailable.'));
    }
    // Draw the exact, piecewise-linear profile. Include out-of-envelope loads
    // in the viewport; clipping a warning point would misrepresent the plan.
    const polygon=[...f,...a.slice().reverse()];
    const xs=[...polygon.map(p=>p.cgIn),cg,gearUpCg], ys=[...polygon.map(p=>p.weightLbs),tow];
    const xmin=Math.min(...xs), xmax=Math.max(...xs), ymin=Math.min(...ys), ymax=Math.max(...ys);
    if (xmin === xmax || ymin === ymax) return paint(target,missing('Envelope scale unavailable.'));
    const x=v=>(59+280*(v-xmin)/(xmax-xmin)).toFixed(2), y=v=>(133-102*(v-ymin)/(ymax-ymin)).toFixed(2);
    const pt=polygon.map(p=>x(p.cgIn)+','+y(p.weightLbs)).join(' ');
    const ink=valid=>valid?'var(--efb-link)':'var(--efb-ifr)';
    const caption=`Takeoff: ${cg.toFixed(2)} in (${takeoffEnvelope.valid?'inside':'outside'}). Gear up: ${gearUpCg.toFixed(2)} in (${gearUpEnvelope.valid?'inside':'outside'}). Weight: ${tow.toFixed(1)} lb.`;
    const gx=Number(x(gearUpCg)), gy=Number(y(tow));
    paint(target,`<svg viewBox="0 0 390 170" aria-hidden="true" focusable="false"><defs><linearGradient id="cg-fill" x2="0" y2="1"><stop stop-color="var(--efb-active)" stop-opacity=".38"/><stop offset="1" stop-color="var(--efb-active)" stop-opacity=".08"/></linearGradient></defs>
      <path d="M53 26v113h295" fill="none" stroke="var(--efb-secondary)"/><text x="5" y="16">lb</text>
      <text x="48" y="35" text-anchor="end">${ymax.toFixed(0)}</text><text x="48" y="136" text-anchor="end">${ymin.toFixed(0)}</text>
      <text x="59" y="160">${xmin.toFixed(2)}</text><text x="205" y="160" text-anchor="middle">CG · in</text><text x="345" y="160" text-anchor="end">${xmax.toFixed(2)}</text>
      <polygon points="${pt}" fill="url(#cg-fill)" stroke="var(--efb-link)" stroke-width="1.5"/>
      <circle cx="${x(cg)}" cy="${y(tow)}" r="11" fill="${ink(takeoffEnvelope.valid)}" opacity=".18"/>
      <circle cx="${x(cg)}" cy="${y(tow)}" r="7" fill="none" stroke="${ink(takeoffEnvelope.valid)}" stroke-width="2.5"/>
      <path d="M${gx} ${gy-4}l4 4 -4 4 -4 -4z" fill="${ink(gearUpEnvelope.valid)}" stroke="var(--efb-card)"/></svg>
      <p class="instrument-caption efb-telemetry">○ Takeoff · ◇ Gear up<br>${caption}</p>`);
  }
  window.PilotInstruments = Object.freeze({
    fuel:(target, values)=>bars(target,['Taxi','Trip','Reserves*','Extra'],values,1,'fuel','No planned fuel entered.'),
    durations:(target, values)=>bars(target,['Plan · flight','Actual · flight','Plan · block','Actual · block'],values,0,'duration','No planned or recorded durations.'),
    envelope,
    limits:(target, profile)=>{
      const rows=[['Forward',profile?.forwardCgLimits],['Aft',profile?.aftCgLimits]];
      if (!rows.every(([,edge])=>Array.isArray(edge) && edge.length && edge.every(p=>finite(p?.cgIn) && finite(p?.weightLbs)))) return paint(target,missing('Profile limits unavailable.'));
      paint(target,rows.flatMap(([label,edge])=>edge.map(p=>`<div class="envelope-point"><span>${label}</span>${p.cgIn.toFixed(2)} IN / ${p.weightLbs} LB</div>`)).join(''));
    }
  });
})();

/* Request inbox is session-only. Provider text is always inserted as text. */
window.createPilotWeatherWorkspace = function ({getRoute}) {
  const el=id=>document.getElementById(id), inbox=el('wxRequestInbox');
  let controller, sequence=0;
  const utc=value=>value && Number.isFinite(Date.parse(value)) ? new Date(value).toISOString().replace('T',' ').slice(0,19)+'Z' : 'Time unknown';
  const text=(tag,value,className)=>{const node=document.createElement(tag);node.textContent=value;if(className)node.className=className;return node;};
  function decode(data) {
    const o=data.observation;
    if (!o) return data.reports.map(r=>r.product==='taf' && r.raw ? window.PilotWx.forecast(r.raw) : r.error || (r.status==='missing'?'No report available for '+r.icao:'No METAR observation to decode.')).join('\n');
    const value=(n,unit)=>Number.isFinite(n)?n+' '+unit:'Not reported';
    return [`${o.category || 'UNKNOWN'} · METAR ${utc(o.observedAt)}`,`Wind: ${o.wind ? (o.wind.variable?'Variable':o.wind.direction+'° true')+' / '+o.wind.speed+' kt'+(o.wind.gust!=null?' gust '+o.wind.gust+' kt':'') : 'Not reported'}`,
      `Visibility: ${value(o.visibilityM,'m')} · Ceiling: ${value(o.ceilingFt,'ft')}`,`Temperature: ${value(o.oatC,'°C')} · Dew point: ${value(o.dewpointC,'°C')} · QNH: ${value(o.qnhHpa,'hPa')}`,
      ...data.reports.filter(r=>r.product==='taf').map(r=>r.raw?window.PilotWx.forecast(r.raw):'TAF: '+(r.error || 'Not available')),
      'Partial decode only. Read the complete raw report and verify validity.'].join('\n');
  }
  function add(data) {
    const card=document.createElement('article');card.className='wx-message';
    card.append(text('h3',data.icao+' · '+data.product.toUpperCase()));
    const cache=[...new Set(data.reports.map(r=>r.cache))].join('/');
    card.append(text('p',`Requested ${utc(data.requestedAt)} · ${(data.elapsedMs/1000).toFixed(2)} s · cache ${cache}`));
    const age=text('p','');age.dataset.wxObservation=data.observation?.observedAt || '';card.append(age);
    for(const report of data.reports) card.append(text('small',`${report.product.toUpperCase()} · ${report.source} · retrieved ${utc(report.retrievedAt)} · ${report.status}`));
    const controls=document.createElement('div');controls.className='wx-mode-control';
    const output=document.createElement('pre');output.className='wx-message-body';
    const raw=data.reports.map(r=>`${r.product.toUpperCase()} ${r.icao}\n${r.raw || r.error || 'NOT AVAILABLE — no station report returned.'}`).join('\n\n');
    for(const mode of ['Raw','Decoded']) {
      const button=text('button',mode);button.type='button';button.setAttribute('aria-pressed',String(mode==='Raw'));
      button.addEventListener('click',()=>{output.textContent=mode==='Raw'?raw:decode(data);controls.querySelectorAll('button').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));});controls.append(button);
    }
    const retry=text('button','Request again');retry.type='button';retry.addEventListener('click',()=>request([data.icao],data.product));
    controls.append(retry);output.textContent=raw;card.append(controls,output);inbox.prepend(card);
    while(inbox.children.length>20)inbox.lastElementChild.remove();updateAges();
  }
  function updateAges() {
    inbox.querySelectorAll('[data-wx-observation]').forEach(node=>{const time=Date.parse(node.dataset.wxObservation),age=Math.floor((Date.now()-time)/60000);node.textContent=Number.isFinite(time)?`Observed ${utc(node.dataset.wxObservation)} · ${Math.max(0,age)} min old${age>75?' · STALE':age< -1?' · TIME INVALID':''}`:'Observation time unavailable · check raw issuance / validity';});
  }
  async function request(codes,product) {
    controller?.abort();controller=new AbortController();const token=++sequence, signal=controller.signal;
    el('wxRequestCancel').hidden=false;el('wxRequestSend').disabled=true;
    el('wxRequestStatus').textContent='Requesting '+codes.join(' / ')+'…';
    const currentController=controller, timeout=setTimeout(()=>currentController.abort(),20000);
    let failures=0;
    await Promise.all(codes.map(async icao=>{
      try {
        const response=await fetch('/api/booking-ops/weather-report?'+new URLSearchParams({icao,product}),{cache:'no-store',signal});
        const data=await response.json();
        if(token!==sequence)return;
        if(!Array.isArray(data.reports))throw new Error(data.error || 'Weather request failed');
        if(!response.ok)failures++;
        add(data);
      } catch(error) {if(token===sequence){failures++;el('wxRequestStatus').textContent=signal.aborted?'Request cancelled or timed out. Retry when ready.':error.message;}}
    }));
    clearTimeout(timeout);
    if(token!==sequence)return;
    if(!signal.aborted)el('wxRequestStatus').textContent=failures?'Some reports failed. See inbox and retry.':'Request complete · newest reports first · last 20 held until page reload.';
    el('wxRequestCancel').hidden=true;el('wxRequestSend').disabled=false;controller=null;
  }
  el('wxRequestForm').addEventListener('submit',event=>{event.preventDefault();const code=el('wxRequestIcao').value.trim().toUpperCase();if(/^[A-Z]{4}$/.test(code))request([code],document.querySelector('[name=wxProduct]:checked').value);});
  el('wxRequestCancel').addEventListener('click',()=>controller?.abort());
  el('wxRequestRoute').addEventListener('click',()=>{const codes=[...new Set(getRoute())].filter(c=>/^[A-Z]{4}$/.test(c));if(codes.length)request(codes,'summary');else el('wxRequestStatus').textContent='Enter briefing airports or select a flight first.';});
  document.querySelectorAll('[data-wx-format]').forEach(button=>button.addEventListener('click',()=>{el('weatherAirportGrid').dataset.format=button.dataset.wxFormat;document.querySelectorAll('[data-wx-format]').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));}));
  return {updateAges,compare(data){
    const target=el('weatherRouteComparison');target.replaceChildren();
    for(const [label,wx] of [['Departure',data.departure],['Destination',data.arrival],['Alternate',data.alternate]]) {
      if(!wx)continue;const card=document.createElement('article');card.className='wx-route-summary';
      card.append(text('h3',label+' · '+wx.icao),text('strong',wx.flightCategory || 'UNKNOWN'),text('p',wx.metarSource?`METAR station ${wx.metarSource}${wx.metarFallback?' · NEARBY REFERENCE ONLY':''} · ${utc(wx.metarObservedAt)}`:'No METAR station report'),text('p',window.PilotWx.forecast(wx.taf)));target.append(card);
    }
  }};
};

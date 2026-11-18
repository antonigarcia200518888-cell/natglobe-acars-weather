/* Mirror the PDF's exact drawing commands for an iPad-safe SVG preview.
   The PDF remains authoritative; preview and export share the same layout pass. */
export function capturePdfPage(page, { width, height, background, courier, courierBold }) {
  const result = { width, height, background, marks: [] };
  const color = value => value ? [value.red, value.green, value.blue].map(v => Math.round(v * 255)) : [20, 20, 20];
  for (const method of ['drawText', 'drawLine', 'drawRectangle']) {
    const original = page[method].bind(page);
    page[method] = (...args) => {
      const options = method === 'drawText' ? args[1] : args[0];
      const { x, y, size, width: w, height: h, start, end, thickness, borderWidth, rotate } = options;
      result.marks.push({ type: method, text: method === 'drawText' ? args[0] : undefined,
        x, y, size, width: w, height: h, start, end, thickness, borderWidth,
        angle: rotate?.angle || 0, color: color(options.color),
        borderColor: color(options.borderColor), fill: Boolean(options.color),
        mono: options.font === courier || options.font === courierBold,
        bold: options.font === courierBold || (options.font && options.font !== courier) });
      return original(...args);
    };
  }
  return result;
}

export function journeyLogLines(request, plan, generatedAt = new Date().toISOString()) {
  const clean = value => String(value || 'NOT RECORDED').replace(/[\r\n\t]+/g, ' ').trim();
  const instant = value => {
    if (!value || !Number.isFinite(Date.parse(value))) return 'NOT RECORDED';
    return new Date(value).toISOString().replace('T', ' ').slice(0, 19) + 'Z';
  };
  const elapsed = (from, to) => {
    const a = Date.parse(from), b = Date.parse(to);
    if (!Number.isFinite(a) || !Number.isFinite(b) || b < a) return 'NOT AVAILABLE';
    const minutes = Math.round((b - a) / 60000);
    return `${String(Math.floor(minutes / 60)).padStart(2, '0')}:${String(minutes % 60).padStart(2, '0')} (${minutes} MIN)`;
  };
  const lines = [
    'NATGLOBE AVIATION', 'AIRCRAFT JOURNEY LOG', '',
    `FLIGHT: ${clean(plan.flightReference || request.id)}`,
    `AIRCRAFT: ${clean(plan.aircraftRegistration)} / ${clean(plan.aircraftModel)}`,
    `ROUTE: ${clean(plan.departure)} -> ${clean(plan.destination)}`,
    `ALTERNATE: ${clean(plan.alternate)}`,
    `COMMANDER: ${clean(plan.commanderName)}`,
    `SECOND PILOT: ${clean(request.crew?.secondary)}`, '',
    'ACTUAL MOVEMENTS / DATED UTC RECORDS',
    'EVENT  DESCRIPTION              UTC DATE & TIME',
    '-----  -----------------------  --------------------'
  ];
  for (const [phase, label] of [['Out', 'Off stand / block out'], ['Off', 'Takeoff / airborne'], ['On', 'Landing / touchdown'], ['In', 'On stand / block in']]) {
    lines.push(`${phase.toUpperCase().padEnd(7)}${label.padEnd(25)}${instant(plan[`actual${phase}Iso`])}`);
    if (!plan[`actual${phase}Iso`] && plan[`actual${phase}`]) lines.push('       Legacy clock-only entry; dated time not inferred.');
  }
  lines.push('', `AIRBORNE: ${elapsed(plan.actualOffIso, plan.actualOnIso)}`,
    `BLOCK:    ${elapsed(plan.actualOutIso, plan.actualInIso)}`, '',
    `PLANNED OFF BLOCK: ${instant(plan.scheduledOutIso)}`,
    `PLANNED AIRBORNE DURATION: ${Number(plan.estimatedEnrouteMinutes) > 0 ? plan.estimatedEnrouteMinutes + ' MIN' : 'NOT RECORDED'}`,
    '', 'OPERATIONAL NOTES', clean(plan.briefingRemarks), '',
    `GENERATED: ${instant(generatedAt)}`, `SOURCE: SAVED FLIGHT ${clean(request.id)}`,
    'Pilot review required. This is not a maintenance release or',
    'an automatically confirmed personal crew logbook entry.');
  // Monospaced wrapping never shrinks text to fit long identifiers or remarks.
  return lines.flatMap(line => line.match(/.{1,78}(?:\s|$)|.{1,78}/g)?.map(part => part.trimEnd()) || ['']);
}

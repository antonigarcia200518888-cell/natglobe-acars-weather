// Manual UTC entries must name a real calendar date. Never infer a rollover.
export function parseMovementUtc(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(value)) return null;
  const date = new Date(value + ':00.000Z');
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 16) !== value) return null;
  return date;
}

export function movementOrderError(plan, changedField, instant) {
  const fields = ['actualOut', 'actualOff', 'actualOn', 'actualIn'];
  const index = fields.indexOf(changedField), time = Date.parse(instant);
  if (index < 0 || !Number.isFinite(time)) return 'Invalid movement timestamp.';
  for (let i = 0; i < fields.length; i++) {
    if (i === index) continue;
    const other = Date.parse(plan[fields[i] + 'Iso']);
    if (!Number.isFinite(other)) continue; // Legacy clock-only records have no inferable date.
    if ((i < index && time < other) || (i > index && time > other)) {
      const label = field => field.replace('actual', '').toUpperCase();
      return `${label(changedField)} must be ${i < index ? 'at or after' : 'at or before'} ${label(fields[i])}. Check the UTC date and time.`;
    }
  }
  return '';
}

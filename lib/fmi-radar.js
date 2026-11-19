import {XMLParser} from 'fast-xml-parser';
export const FMI_WMS = 'https://openwms.fmi.fi/geoserver/wms';
export const FMI_LAYER = 'Radar:radar_finland_cappi_dbzh';
export const FMI_STYLE = 'radar_dbz_winter_-20_40_nodata';
export function radarManifest(xml, now = Date.now()) {
  const root = new XMLParser({ignoreAttributes:false}).parse(xml);
  function find(node) {
    if (!node || typeof node !== 'object') return null;
    if (node.Name === FMI_LAYER) return node;
    for (const value of Object.values(node)) {
      if (value && typeof value === 'object') { const hit = find(value); if (hit) return hit; }
    }
    return null;
  }
  const layer = find(root);
  const dimensions = [].concat(layer?.Dimension || []);
  const dimension = dimensions.find(value => value['@_name'] === 'time');
  const times = new Set();
  for (const part of String(dimension?.['#text'] || '').split(',')) {
    const [start, end, period] = part.trim().split('/');
    if (end) {
      const match = /^PT(\d+)M$/.exec(period || '');
      if (!match || Number(match[1]) < 1) continue;
      const step = Number(match[1]) * 60000;
      const latest = Math.min(Date.parse(end), now);
      const first = Date.parse(start);
      if (!Number.isFinite(first) || !Number.isFinite(latest)) continue;
      const aligned = first + Math.floor((latest - first) / step) * step;
      for (let time = aligned; time >= Math.max(first, aligned - 2 * 3600000); time -= step) times.add(time);
    } else { const time = Date.parse(start); if (Number.isFinite(time) && time <= now) times.add(time); }
  }
  const frames = [...times].sort((a,b) => a-b).slice(-25).map(time => ({time:time/1000, iso:new Date(time).toISOString(), kind:'observed'}));
  if (!frames.length) throw new Error('FMI radar has no valid observation frames');
  return {provider:'Finnish Meteorological Institute', license:'CC BY 4.0', coverage:'Finland radar network — not worldwide coverage',
    generatedAt:new Date(now).toISOString(), host:FMI_WMS, layer:FMI_LAYER, style:FMI_STYLE, frames,
    bounds:[[57.1,15.3],[70.8,42.7]],
    legend:`${FMI_WMS}?service=WMS&version=1.3.0&request=GetLegendGraphic&format=image%2Fpng&layer=${encodeURIComponent(FMI_LAYER)}&style=${FMI_STYLE}`};
}
export function createRadarService({fetchImpl = fetch, now = Date.now} = {}) {
  let cached, expires = 0, pending;
  return async () => {
    if (cached && now() < expires) return cached;
    if (pending) return pending;
    pending = (async () => {
      const response = await fetchImpl(`${FMI_WMS}?service=WMS&version=1.3.0&request=GetCapabilities`, {signal:AbortSignal.timeout(8000)});
      if (!response.ok) throw new Error(`FMI radar metadata HTTP ${response.status}`);
      cached = radarManifest(await response.text(), now()); expires = now() + 120000; return cached;
    })().finally(() => {pending = null;});
    return pending;
  };
}

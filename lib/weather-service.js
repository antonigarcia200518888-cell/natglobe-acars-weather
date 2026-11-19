// Server-side AWC client. Batch simultaneous station requests, share in-flight
// work, bound memory, and keep missing reports distinct from provider errors.
export function createWeatherService({fetchImpl = fetch, now = Date.now, batchMs = 40, spacingMs = 700, timeoutMs = 6500} = {}) {
  const cache = new Map(), pending = new Map(), queues = {metar:new Map(), taf:new Map()};
  let timer, nextStart = 0;
  const put = (key, value, ttl) => {
    cache.delete(key); cache.set(key, {value, expires:now() + ttl});
    while (cache.size > 512) cache.delete(cache.keys().next().value);
  };
  async function batch(product, entries) {
    const delay = Math.max(0, nextStart - now()); nextStart = Math.max(now(), nextStart) + spacingMs;
    if (delay) await new Promise(resolve => setTimeout(resolve, delay));
    let rows, error;
    try {
      const response = await fetchImpl(`https://aviationweather.gov/api/data/${product}?ids=${entries.map(([icao]) => icao).join(',')}&format=json`, {
        headers:{'User-Agent':'NatGlobeAviation-EFB/2.0'}, signal:AbortSignal.timeout(timeoutMs)
      });
      if (!response.ok) throw new Error(`Weather provider returned HTTP ${response.status}`);
      rows = response.status === 204 ? [] : await response.json();
      if (!Array.isArray(rows)) throw new Error('Weather provider returned an invalid response');
    } catch (cause) { error = cause.name === 'TimeoutError' ? 'Weather provider timed out. Retry.' : cause.message; }
    for (const [icao, resolve] of entries) {
      const row = rows?.find(item => item.icaoId === icao);
      const raw = String(row?.[product === 'metar' ? 'rawOb' : 'rawTAF'] || '').trim();
      const value = {icao, product, raw, status:error ? 'error' : raw ? 'ok' : 'missing', error:error || null,
        source:'NOAA Aviation Weather Center', retrievedAt:new Date(now()).toISOString()};
      put(`${product}:${icao}`, value, error ? 10000 : raw ? 90000 : 30000);
      resolve(value); pending.delete(`${product}:${icao}`);
    }
  }
  function flush() {
    timer = null;
    for (const product of ['metar','taf']) {
      const entries = [...queues[product]]; queues[product].clear();
      if (entries.length) void batch(product, entries);
    }
  }
  async function get(product, input) {
    const icao = String(input || '').toUpperCase();
    if (!['metar','taf'].includes(product) || !/^[A-Z]{4}$/.test(icao)) throw new Error('Use a four-letter ICAO and METAR or TAF');
    const started = now(), key = `${product}:${icao}`, existing = cache.get(key);
    if (existing?.expires > now()) return {...existing.value, cache:'hit', elapsedMs:now() - started};
    let promise = pending.get(key), mode = 'shared';
    if (!promise) {
      if (pending.size >= 96 || nextStart - now() > 10000) return {icao,product,raw:'',status:'error',error:'Weather service is busy. Retry shortly.',source:'NOAA Aviation Weather Center',retrievedAt:new Date(now()).toISOString(),cache:'none',elapsedMs:0};
      mode = 'miss'; promise = new Promise(resolve => queues[product].set(icao, resolve));
      pending.set(key, promise); if (!timer) timer = setTimeout(flush, batchMs);
    }
    return {...await promise, cache:mode, elapsedMs:now() - started};
  }
  return {get};
}

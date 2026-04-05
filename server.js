import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import { airports as bundledAirports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const AIRPORT_DB_TTL_MS = 24 * 60 * 60 * 1000;
const WEATHER_CACHE_TTL_MS = 90 * 1000;
const NEGATIVE_CACHE_TTL_MS = 15 * 1000;

const FETCH_TIMEOUT_RAW_MS = 4500;
const FETCH_TIMEOUT_AIRPORT_DB_MS = 10000;
const FETCH_TIMEOUT_WINDS_MS = 5500;

const FALLBACK_SEARCH_LIMIT = 16;
const FALLBACK_BATCH_SIZE = 4;

const OURAIRPORTS_CSV_URL = 'https://davidmegginson.github.io/ourairports-data/airports.csv';

let airportDbCache = {
  data: null,
  loadedAt: 0,
  promise: null
};

const responseCache = new Map();

function getCached(key) {
  const item = responseCache.get(key);
  if (!item) return undefined;

  if (Date.now() > item.expiresAt) {
    responseCache.delete(key);
    return undefined;
  }

  return item.value;
}

function setCached(key, value, ttlMs) {
  responseCache.set(key, {
    value,
    expiresAt: Date.now() + ttlMs
  });
}

function normalizeIcao(input) {
  return String(input || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, '')
    .slice(0, 4);
}

function normalizeFlight(input) {
  return String(input || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, 10);
}

function normalizeRoute(input) {
  return String(input || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9/ \-]/g, '')
    .replace(/\s+/g, ' ')
    .slice(0, 40);
}

function normalizeRemarks(input) {
  return String(input || '')
    .trim()
    .replace(/\s+/g, ' ')
    .toUpperCase()
    .slice(0, 80);
}

function parseBoolean(v) {
  return String(v).toLowerCase() === 'true';
}

function haversineKm(a, b) {
  const R = 6371;
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = ((b.lon - a.lon) * Math.PI) / 180;
  const lat1 = (a.lat * Math.PI) / 180;
  const lat2 = (b.lat * Math.PI) / 180;

  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;

  return 2 * R * Math.asin(Math.sqrt(x));
}

function kmToNm(km) {
  return km / 1.852;
}

function formatNm(km) {
  if (km == null || Number.isNaN(km)) return null;
  return Math.round(kmToNm(km));
}

function parseCsv(csvText) {
  const rows = [];
  let row = [];
  let cell = '';
  let inQuotes = false;

  for (let i = 0; i < csvText.length; i++) {
    const ch = csvText[i];
    const next = csvText[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === ',' && !inQuotes) {
      row.push(cell);
      cell = '';
      continue;
    }

    if ((ch === '\n' || ch === '\r') && !inQuotes) {
      if (ch === '\r' && next === '\n') i++;
      row.push(cell);
      if (row.some(value => value !== '')) rows.push(row);
      row = [];
      cell = '';
      continue;
    }

    cell += ch;
  }

  if (cell.length || row.length) {
    row.push(cell);
    if (row.some(value => value !== '')) rows.push(row);
  }

  if (!rows.length) return [];

  const headers = rows[0];
  return rows.slice(1).map(cols => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = cols[i] ?? '';
    }
    return obj;
  });
}

function normalizeAirportRow(row) {
  const icao = String(row.gps_code || row.ident || '')
    .trim()
    .toUpperCase();

  const lat = Number(row.latitude_deg);
  const lon = Number(row.longitude_deg);
  const elevationFt = Number(row.elevation_ft);
  const country = String(row.iso_country || '').trim().toUpperCase();

  if (!/^[A-Z]{4}$/.test(icao)) return null;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (!['FI', 'EE'].includes(country)) return null;

  return {
    icao,
    name: String(row.name || '').trim(),
    municipality: String(row.municipality || '').trim(),
    country,
    type: String(row.type || '').trim(),
    lat,
    lon,
    elevationFt: Number.isFinite(elevationFt) ? elevationFt : null
  };
}

function getBundledAirportFallback() {
  return (bundledAirports || [])
    .filter(a => a && /^[A-Z]{4}$/.test(String(a.icao || '').toUpperCase()))
    .filter(a => Number.isFinite(Number(a.lat)) && Number.isFinite(Number(a.lon)))
    .map(a => ({
      icao: String(a.icao).toUpperCase(),
      name: String(a.name || '').trim(),
      municipality: String(a.city || '').trim(),
      country: String(a.country || '').trim().toUpperCase(),
      type: String(a.type || 'airport').trim(),
      lat: Number(a.lat),
      lon: Number(a.lon),
      elevationFt: Number.isFinite(Number(a.elevationFt)) ? Number(a.elevationFt) : null
    }));
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 5000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchCsvAirports(url) {
  const res = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
    FETCH_TIMEOUT_AIRPORT_DB_MS
  );

  if (!res.ok) {
    throw new Error(`AIRPORT DB HTTP ${res.status}`);
  }

  const csv = await res.text();
  return parseCsv(csv)
    .map(normalizeAirportRow)
    .filter(Boolean);
}

async function loadAirportDatabase() {
  const now = Date.now();

  if (airportDbCache.data && now - airportDbCache.loadedAt < AIRPORT_DB_TTL_MS) {
    return airportDbCache.data;
  }

  if (airportDbCache.promise) {
    return airportDbCache.promise;
  }

  airportDbCache.promise = (async () => {
    try {
      const remoteAirports = await fetchCsvAirports(OURAIRPORTS_CSV_URL);
      const mergedMap = new Map();

      for (const apt of [...getBundledAirportFallback(), ...remoteAirports]) {
        if (!mergedMap.has(apt.icao)) mergedMap.set(apt.icao, apt);
      }

      const data = Array.from(mergedMap.values());
      airportDbCache.data = data;
      airportDbCache.loadedAt = Date.now();
      airportDbCache.promise = null;
      console.log(`Loaded airport DB: ${data.length} FI+EE aerodromes`);
      return data;
    } catch (err) {
      console.warn('Failed to load remote airport DB, using bundled fallback:', err.message);
      const data = getBundledAirportFallback();
      airportDbCache.data = data;
      airportDbCache.loadedAt = Date.now();
      airportDbCache.promise = null;
      return data;
    }
  })();

  return airportDbCache.promise;
}

async function findAirportByIcao(icao) {
  const airportDb = await loadAirportDatabase();
  return airportDb.find(a => a.icao === icao) || null;
}

async function getNearbyAirports(icao, limit = FALLBACK_SEARCH_LIMIT) {
  const airportDb = await loadAirportDatabase();
  const target = airportDb.find(a => a.icao === icao);

  if (!target) return [];

  return airportDb
    .filter(a => a.icao !== icao)
    .map(a => {
      const distanceKm = haversineKm(target, a);
      return {
        ...a,
        distanceKm,
        distanceNm: formatNm(distanceKm)
      };
    })
    .sort((a, b) => a.distanceKm - b.distanceKm)
    .slice(0, limit);
}

async function fetchRaw(url) {
  const cacheKey = `raw:${url}`;
  const cached = getCached(cacheKey);
  if (cached !== undefined) return cached;

  try {
    const res = await fetchWithTimeout(
      url,
      { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
      FETCH_TIMEOUT_RAW_MS
    );

    if (res.status === 204) {
      setCached(cacheKey, null, WEATHER_CACHE_TTL_MS);
      return null;
    }

    if (!res.ok) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const text = String(await res.text() || '').trim() || null;
    setCached(cacheKey, text, WEATHER_CACHE_TTL_MS);
    return text;
  } catch {
    return null;
  }
}

async function getMetar(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);
}

async function getTaf(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);
}

async function getNearestOfficial(fetchFn, requestedIcao) {
  const nearby = await getNearbyAirports(requestedIcao, FALLBACK_SEARCH_LIMIT);

  for (let i = 0; i < nearby.length; i += FALLBACK_BATCH_SIZE) {
    const batch = nearby.slice(i, i + FALLBACK_BATCH_SIZE);

    const results = await Promise.all(
      batch.map(async (apt) => {
        const found = await fetchFn(apt.icao);
        if (!found) return null;

        return {
          text: found,
          source: apt.icao,
          fallback: true,
          distanceKm: apt.distanceKm,
          distanceNm: apt.distanceNm
        };
      })
    );

    const firstHit = results.find(Boolean);
    if (firstHit) return firstHit;
  }

  return {
    text: 'NOT AVAILABLE',
    source: null,
    fallback: false,
    distanceKm: null,
    distanceNm: null
  };
}

function parseWindKt(metar) {
  const gustMatch = metar.match(/\b(\d{3}|VRB)(\d{2,3})G(\d{2,3})KT\b/);
  if (gustMatch) {
    return {
      speed: parseInt(gustMatch[2], 10),
      gust: parseInt(gustMatch[3], 10)
    };
  }

  const basicMatch = metar.match(/\b(\d{3}|VRB)(\d{2,3})KT\b/);
  if (basicMatch) {
    return {
      speed: parseInt(basicMatch[2], 10),
      gust: null
    };
  }

  return null;
}

function parseVisibility(metar) {
  const visMatch = metar.match(/\b(\d{4})\b/);
  if (!visMatch) return null;

  const value = parseInt(visMatch[1], 10);
  if (Number.isNaN(value)) return null;

  return value;
}

function parseWeatherPhenomena(metar) {
  const phenomena = [];
  const tokens = ['FG', 'BR', 'RA', 'SN', 'TS', 'DZ', 'FZRA', 'SHRA', 'SHSN'];

  for (const token of tokens) {
    if (metar.includes(token)) phenomena.push(token);
  }

  return phenomena;
}

function parseCloudBase(metar) {
  const cloudRegex = /\b(FEW|SCT|BKN|OVC)(\d{3})\b/g;
  let match;
  let lowest = null;

  while ((match = cloudRegex.exec(metar)) !== null) {
    const base = parseInt(match[2], 10) * 100;
    if (!Number.isNaN(base)) {
      if (lowest === null || base < lowest) lowest = base;
    }
  }

  return lowest;
}

function parseTemperatureC(metar) {
  const match = metar.match(/\b(M?\d{2})\/(M?\d{2})\b/);
  if (!match) return null;

  const raw = match[1];
  const isMinus = raw.startsWith('M');
  const num = parseInt(raw.replace('M', ''), 10);

  if (Number.isNaN(num)) return null;
  return isMinus ? -num : num;
}

function arrivalNeedsAlternate(wx) {
  if (!wx || !wx.metar || wx.metar === 'NOT AVAILABLE') return false;

  const metar = String(wx.metar);
  const wind = parseWindKt(metar);
  const vis = parseVisibility(metar);
  const phenomena = parseWeatherPhenomena(metar);
  const cloudBase = parseCloudBase(metar);

  if (vis !== null && vis < 5000) return true;
  if (cloudBase !== null && cloudBase <= 1000) return true;
  if (phenomena.includes('TS') || phenomena.includes('FG')) return true;
  if (wind?.gust && wind.gust >= 25) return true;

  return false;
}

function pushUnique(remarks, value) {
  if (!remarks.includes(value)) remarks.push(value);
}

function generateAutoRemark(data) {
  if (data.remarks) return data.remarks;

  const remarks = [];

  function inspect(wx) {
    if (!wx || !wx.metar || wx.metar === 'NOT AVAILABLE') return;

    const metar = String(wx.metar);
    const wind = parseWindKt(metar);
    const vis = parseVisibility(metar);
    const phenomena = parseWeatherPhenomena(metar);
    const cloudBase = parseCloudBase(metar);
    const tempC = parseTemperatureC(metar);

    if (vis !== null) {
      if (vis < 1500) {
        pushUnique(remarks, 'VERY LOW VIS');
      } else if (vis < 2000) {
        pushUnique(remarks, 'POOR VIS');
      } else if (vis < 5000) {
        pushUnique(remarks, 'LOW VIS');
      }
    }

    if (wind) {
      if (wind.gust && wind.gust >= 25) {
        pushUnique(remarks, `GUSTS ${wind.gust}KT`);
      } else if (wind.speed >= 20) {
        pushUnique(remarks, 'STRONG WIND');
      }

      if (wind.gust && wind.gust - wind.speed >= 10) {
        pushUnique(remarks, 'GUST SPREAD');
      }
    }

    if (cloudBase !== null) {
      if (cloudBase <= 500) {
        pushUnique(remarks, 'VERY LOW CEILING');
      } else if (cloudBase <= 2000) {
        pushUnique(remarks, 'LOW CEILING');
      }
    }

    if (phenomena.includes('TS')) {
      pushUnique(remarks, 'TS');
    }

    if (phenomena.includes('FG')) {
      pushUnique(remarks, 'FOG');
    } else if (phenomena.includes('BR')) {
      pushUnique(remarks, 'MIST');
    }

    if (phenomena.includes('FZRA')) {
      pushUnique(remarks, 'FREEZING PRECIP');
    } else if (
      phenomena.includes('RA') ||
      phenomena.includes('SHRA') ||
      phenomena.includes('DZ')
    ) {
      pushUnique(remarks, 'PRECIP');
    }

    if (phenomena.includes('SN') || phenomena.includes('SHSN')) {
      pushUnique(remarks, 'SNOW');
    }

    if (
      tempC !== null &&
      tempC <= 2 &&
      (
        phenomena.includes('RA') ||
        phenomena.includes('SHRA') ||
        phenomena.includes('DZ') ||
        phenomena.includes('FZRA') ||
        phenomena.includes('SN') ||
        phenomena.includes('SHSN') ||
        (cloudBase !== null && cloudBase <= 2000)
      )
    ) {
      pushUnique(remarks, 'ICING RISK');
    }
  }

  inspect(data.depWx);
  inspect(data.arrWx);

  if (arrivalNeedsAlternate(data.arrWx)) {
    pushUnique(remarks, 'ALTN REQUIRED');
  }

  return remarks.length ? remarks.join(' / ') : 'NIL';
}

async function getAirportWeather(icao) {
  if (!icao) return null;

  const [officialMetar, officialTaf] = await Promise.all([
    getMetar(icao),
    getTaf(icao)
  ]);

  let metar = officialMetar;
  let taf = officialTaf;
  let metarSource = officialMetar ? icao : null;
  let tafSource = officialTaf ? icao : null;
  let metarFallback = false;
  let tafFallback = false;
  let metarDistanceNm = 0;
  let tafDistanceNm = 0;
  let mode = 'LIVE';

  const fallbackPromises = [];
  let needFallbackMetar = false;
  let needFallbackTaf = false;

  if (!metar) {
    needFallbackMetar = true;
    fallbackPromises.push(getNearestOfficial(getMetar, icao));
  }

  if (!taf) {
    needFallbackTaf = true;
    fallbackPromises.push(getNearestOfficial(getTaf, icao));
  }

  if (fallbackPromises.length) {
    const results = await Promise.all(fallbackPromises);
    let resultIndex = 0;

    if (needFallbackMetar) {
      const fallbackMetar = results[resultIndex++];
      metar = fallbackMetar.text;
      metarSource = fallbackMetar.source;
      metarFallback = fallbackMetar.fallback;
      metarDistanceNm = fallbackMetar.distanceNm;
      if (fallbackMetar.fallback) mode = 'FALLBACK';
    }

    if (needFallbackTaf) {
      const fallbackTaf = results[resultIndex++];
      taf = fallbackTaf.text;
      tafSource = fallbackTaf.source;
      tafFallback = fallbackTaf.fallback;
      tafDistanceNm = fallbackTaf.distanceNm;
      if (fallbackTaf.fallback) mode = 'FALLBACK';
    }
  }

  if (!metar) metar = 'NOT AVAILABLE

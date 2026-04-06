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
const SUGGESTED_ALTN_LIMIT = 12;

const OURAIRPORTS_CSV_URL = 'https://davidmegginson.github.io/ourairports-data/airports.csv';
const OURAIRPORTS_RUNWAYS_CSV_URL = 'https://davidmegginson.github.io/ourairports-data/runways.csv';

const EUROPE_COUNTRIES = new Set([
  'AL', 'AD', 'AT', 'BE', 'BA', 'BG', 'BY', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE',
  'ES', 'FI', 'FO', 'FR', 'GB', 'GG', 'GI', 'GR', 'HR', 'HU', 'IE', 'IM', 'IS',
  'IT', 'JE', 'LI', 'LT', 'LU', 'LV', 'MC', 'MD', 'ME', 'MK', 'MT', 'NL', 'NO',
  'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'SM', 'UA', 'VA'
]);

const AIRCRAFT_PROFILE = {
  registration: 'OH-PMK',
  type: 'Piper PA-28R-200 Arrow II',
  bestGlideSpeedKt: 91,
  bestGlideRatio: 9,
  defaultCruiseAltitudeFt: 8000,
  maxCeilingFt: 17000,
  basicEmptyWeightLbs: 1727,
  maxTakeoffWeightLbs: 2650,
  maxLandingWeightLbs: 2650,
  maxRampWeightLbs: 2657,
  fuelType: '100LL',
  startTaxiTakeoffFuelGal: 1.17,
  climbTasKt: 95,
  climbFuelFlowGph: 10,
  climbRateFpm: 600,
  cruiseTasKt: 115,
  cruiseFuelFlowGph: 8,
  descentTasKt: 115,
  descentFuelFlowGph: 8,
  descentRateFpm: 500,
  takeoff: {
    powerSetting: 'FULL',
    ac: 'OFF',
    defaultFlaps: '0°',
    rotatePitch: '7–9° NOSE UP',
    xwindLimitKt: 17,
    limitCode: 'MTOW'
  },
  landing: {
    ac: 'OFF',
    defaultFlaps: 'FULL (40°)',
    braking: 'MAX/MOD/CRM',
    goAroundPower: 'FULL',
    limitCode: 'MLDW'
  }
};

let airportDbCache = {
  data: null,
  loadedAt: 0,
  promise: null
};

let runwayDbCache = {
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

function normalizeHeading(input) {
  const digits = String(input || '').replace(/[^0-9]/g, '').slice(0, 3);
  if (!digits) return null;

  const value = parseInt(digits, 10);
  if (!Number.isFinite(value)) return null;
  if (value < 0 || value > 360) return null;

  return value === 360 ? 0 : value;
}

function normalizeWeight(input) {
  const num = Number(input);
  return Number.isFinite(num) ? Math.round(num) : null;
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
  if (!EUROPE_COUNTRIES.has(country)) return null;

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
    }))
    .filter(a => EUROPE_COUNTRIES.has(a.country));
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

function normalizeRunwayEndIdent(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '');
}

function normalizeSurface(value) {
  return String(value || '')
    .trim()
    .toUpperCase();
}

function roundMaybe(value, digits = 2) {
  if (!Number.isFinite(value)) return null;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function normalizeRunwayRow(row) {
  const airportIcao = normalizeIcao(row.airport_ident || '');
  if (!airportIcao) return null;

  const lengthFt = Number(row.length_ft);
  const widthFt = Number(row.width_ft);
  const slopePct = Number(row.slope);
  const surface = normalizeSurface(row.surface);

  const leIdent = normalizeRunwayEndIdent(row.le_ident);
  const heIdent = normalizeRunwayEndIdent(row.he_ident);

  const leHeading = Number(row.le_heading_degT);
  const heHeading = Number(row.he_heading_degT);

  const leElevationFt = Number(row.le_elevation_ft);
  const heElevationFt = Number(row.he_elevation_ft);

  const leDisplacedFt = Number(row.le_displaced_threshold_ft);
  const heDisplacedFt = Number(row.he_displaced_threshold_ft);

  return {
    airportIcao,
    airportRef: String(row.airport_ref || '').trim(),
    lengthFt: Number.isFinite(lengthFt) ? Math.round(lengthFt) : null,
    widthFt: Number.isFinite(widthFt) ? Math.round(widthFt) : null,
    surface,
    slopePct: Number.isFinite(slopePct) ? roundMaybe(slopePct, 2) : null,
    le: leIdent ? {
      ident: leIdent,
      headingDegT: Number.isFinite(leHeading) ? Math.round(leHeading) : null,
      elevationFt: Number.isFinite(leElevationFt) ? Math.round(leElevationFt) : null,
      displacedThresholdFt: Number.isFinite(leDisplacedFt) ? Math.round(leDisplacedFt) : 0
    } : null,
    he: heIdent ? {
      ident: heIdent,
      headingDegT: Number.isFinite(heHeading) ? Math.round(heHeading) : null,
      elevationFt: Number.isFinite(heElevationFt) ? Math.round(heElevationFt) : null,
      displacedThresholdFt: Number.isFinite(heDisplacedFt) ? Math.round(heDisplacedFt) : 0
    } : null
  };
}

async function fetchCsvRunways(url) {
  const res = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
    FETCH_TIMEOUT_AIRPORT_DB_MS
  );

  if (!res.ok) {
    throw new Error(`RUNWAY DB HTTP ${res.status}`);
  }

  const csv = await res.text();
  return parseCsv(csv)
    .map(normalizeRunwayRow)
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
      console.log(`Loaded airport DB: ${data.length} European aerodromes`);
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

async function loadRunwayDatabase() {
  const now = Date.now();

  if (runwayDbCache.data && now - runwayDbCache.loadedAt < AIRPORT_DB_TTL_MS) {
    return runwayDbCache.data;
  }

  if (runwayDbCache.promise) {
    return runwayDbCache.promise;
  }

  runwayDbCache.promise = (async () => {
    try {
      const airportDb = await loadAirportDatabase();
      const europeIcaoSet = new Set(airportDb.map(a => a.icao));

      const remoteRunways = await fetchCsvRunways(OURAIRPORTS_RUNWAYS_CSV_URL);
      const data = remoteRunways.filter(r => europeIcaoSet.has(r.airportIcao));

      runwayDbCache.data = data;
      runwayDbCache.loadedAt = Date.now();
      runwayDbCache.promise = null;
      console.log(`Loaded runway DB: ${data.length} European runway records`);
      return data;
    } catch (err) {
      console.warn('Failed to load runway DB:', err.message);
      const data = [];
      runwayDbCache.data = data;
      runwayDbCache.loadedAt = Date.now();
      runwayDbCache.promise = null;
      return data;
    }
  })();

  return runwayDbCache.promise;
}

async function findAirportByIcao(icao) {
  const airportDb = await loadAirportDatabase();
  return airportDb.find(a => a.icao === icao) || null;
}

function buildRunwayDirectionEntry(airport, parentRunway, end, side) {
  if (!end?.ident) return null;

  const opposite = side === 'LE' ? parentRunway.he : parentRunway.le;
  const totalLengthFt = Number.isFinite(parentRunway.lengthFt) ? parentRunway.lengthFt : null;
  const displacedFt = Number.isFinite(end.displacedThresholdFt) ? end.displacedThresholdFt : 0;

  const ldaFt = Number.isFinite(totalLengthFt)
    ? Math.max(0, totalLengthFt - displacedFt)
    : null;

  return {
    airport: airport?.icao || parentRunway.airportIcao,
    ident: end.ident,
    heading: Number.isFinite(end.headingDegT) ? end.headingDegT : null,
    reciprocalIdent: opposite?.ident || '',
    lengthFt: totalLengthFt,
    toraFt: totalLengthFt,
    ldaFt,
    elevationFt: Number.isFinite(end.elevationFt)
      ? end.elevationFt
      : (Number.isFinite(airport?.elevationFt) ? airport.elevationFt : null),
    slopePct: Number.isFinite(parentRunway.slopePct)
      ? (side === 'LE' ? parentRunway.slopePct : roundMaybe(-parentRunway.slopePct, 2))
      : null,
    surface: parentRunway.surface || '',
    widthFt: Number.isFinite(parentRunway.widthFt) ? parentRunway.widthFt : null
  };
}

async function getAirportRunways(icao) {
  const normalizedIcao = normalizeIcao(icao);
  if (!normalizedIcao) return [];

  const [airport, runwayDb] = await Promise.all([
    findAirportByIcao(normalizedIcao),
    loadRunwayDatabase()
  ]);

  if (!airport) return [];

  const records = runwayDb
    .filter(r => r.airportIcao === normalizedIcao)
    .flatMap(r => [
      buildRunwayDirectionEntry(airport, r, r.le, 'LE'),
      buildRunwayDirectionEntry(airport, r, r.he, 'HE')
    ])
    .filter(Boolean)
    .sort((a, b) => {
      const aNum = parseInt(String(a.ident).replace(/[^\d]/g, ''), 10);
      const bNum = parseInt(String(b.ident).replace(/[^\d]/g, ''), 10);

      if (Number.isFinite(aNum) && Number.isFinite(bNum) && aNum !== bNum) {
        return aNum - bNum;
      }

      return String(a.ident).localeCompare(String(b.ident));
    });

  return records;
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

function parseWindDetailed(metar) {
  const gustMatch = String(metar || '').match(/\b(\d{3}|VRB)(\d{2,3})G(\d{2,3})KT\b/);
  if (gustMatch) {
    return {
      raw: gustMatch[0],
      direction: gustMatch[1] === 'VRB' ? null : parseInt(gustMatch[1], 10),
      speed: parseInt(gustMatch[2], 10),
      gust: parseInt(gustMatch[3], 10),
      variable: gustMatch[1] === 'VRB'
    };
  }

  const basicMatch = String(metar || '').match(/\b(\d{3}|VRB)(\d{2,3})KT\b/);
  if (basicMatch) {
    return {
      raw: basicMatch[0],
      direction: basicMatch[1] === 'VRB' ? null : parseInt(basicMatch[1], 10),
      speed: parseInt(basicMatch[2], 10),
      gust: null,
      variable: basicMatch[1] === 'VRB'
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

function parseQnhHpa(metar) {
  const match = String(metar || '').match(/\bQ(\d{4})\b/);
  if (!match) return null;

  const value = parseInt(match[1], 10);
  return Number.isFinite(value) ? value : null;
}

function hpaToInHg(hpa) {
  if (!Number.isFinite(hpa)) return null;
  return Math.round((hpa * 0.0295299830714) * 100) / 100;
}

function computePressureAltitudeFt(elevationFt, qnhHpa) {
  if (!Number.isFinite(elevationFt) || !Number.isFinite(qnhHpa)) return null;
  return Math.round(elevationFt + ((1013.25 - qnhHpa) * 27));
}

function computeDensityAltitudeFt(elevationFt, oatC, qnhHpa) {
  if (!Number.isFinite(elevationFt) || !Number.isFinite(oatC) || !Number.isFinite(qnhHpa)) {
    return null;
  }

  const pressureAltitudeFt = computePressureAltitudeFt(elevationFt, qnhHpa);
  if (!Number.isFinite(pressureAltitudeFt)) return null;

  const isaTempC = 15 - (2 * (pressureAltitudeFt / 1000));
  return Math.round(pressureAltitudeFt + (120 * (oatC - isaTempC)));
}

function normalizeDegrees(deg) {
  if (!Number.isFinite(deg)) return null;
  let value = deg % 360;
  if (value < 0) value += 360;
  return value;
}

function computeWindComponents(windDirectionDeg, windSpeedKt, runwayHeadingDeg) {
  if (!Number.isFinite(windDirectionDeg) || !Number.isFinite(windSpeedKt) || !Number.isFinite(runwayHeadingDeg)) {
    return {
      headwindKt: null,
      crosswindKt: null
    };
  }

  const diffDeg = normalizeDegrees(windDirectionDeg - runwayHeadingDeg);
  const angleRad = (diffDeg * Math.PI) / 180;

  return {
    headwindKt: Math.round(windSpeedKt * Math.cos(angleRad)),
    crosswindKt: Math.round(Math.abs(windSpeedKt * Math.sin(angleRad)))
  };
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

  if (!metar) metar = 'NOT AVAILABLE';
  if (!taf) taf = 'NOT AVAILABLE';

  if (metar === 'NOT AVAILABLE' && taf === 'NOT AVAILABLE') {
    mode = 'NO DATA';
  }

  return {
    airport: icao,
    mode,
    metar,
    taf,
    metarSource,
    tafSource,
    metarFallback,
    tafFallback,
    metarDistanceNm,
    tafDistanceNm
  };
}

function wrapText(text, maxChars = 34) {
  const words = String(text || '').split(/\s+/).filter(Boolean);
  const lines = [];
  let current = '';

  for (const word of words) {
    const test = current ? `${current} ${word}` : word;
    if (test.length <= maxChars) {
      current = test;
    } else {
      if (current) lines.push(current);

      if (word.length > maxChars) {
        for (let i = 0; i < word.length; i += maxChars) {
          lines.push(word.slice(i, i + maxChars));
        }
        current = '';
      } else {
        current = word;
      }
    }
  }

  if (current) lines.push(current);
  return lines.length ? lines : [''];
}

function formatDateOnly(d) {
  return d.toISOString().slice(0, 10);
}

function formatUtcClock(d) {
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${hh}:${mm} UTC`;
}

function pickNearestHourlyIndex(times) {
  if (!Array.isArray(times) || !times.length) return -1;

  const now = Date.now();
  let bestIndex = -1;
  let bestDiff = Infinity;

  for (let i = 0; i < times.length; i++) {
    const t = Date.parse(times[i]);
    if (Number.isNaN(t)) continue;

    const diff = t - now;
    if (diff >= 0 && diff < bestDiff) {
      bestDiff = diff;
      bestIndex = i;
    }
  }

  if (bestIndex !== -1) return bestIndex;
  return times.length - 1;
}

function formatWindAloft(directionDeg, speedKt) {
  if (!Number.isFinite(directionDeg) || !Number.isFinite(speedKt)) return null;

  let dir = Math.round(directionDeg / 10) * 10;
  if (dir === 360) dir = 0;

  const ddd = String(dir).padStart(3, '0');
  const ss = String(Math.round(speedKt)).padStart(2, '0');
  return `${ddd}${ss}KT`;
}

async function getWindsAloftForAirport(icao) {
  if (!icao) return null;

  const airport = await findAirportByIcao(icao);
  if (!airport || !Number.isFinite(airport.lat) || !Number.isFinite(airport.lon)) return null;

  const url = new URL('https://api.open-meteo.com/v1/gfs');
  url.searchParams.set('latitude', String(airport.lat));
  url.searchParams.set('longitude', String(airport.lon));
  url.searchParams.set(
    'hourly',
    [
      'wind_speed_925hPa',
      'wind_direction_925hPa',
      'wind_speed_850hPa',
      'wind_direction_850hPa',
      'wind_speed_700hPa',
      'wind_direction_700hPa'
    ].join(',')
  );
  url.searchParams.set('wind_speed_unit', 'kn');
  url.searchParams.set('timezone', 'UTC');
  url.searchParams.set('forecast_days', '2');

  const cacheKey = `winds:${url.toString()}`;
  const cached = getCached(cacheKey);
  if (cached !== undefined) return cached;

  try {
    const res = await fetchWithTimeout(
      url.toString(),
      { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
      FETCH_TIMEOUT_WINDS_MS
    );

    if (!res.ok) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const json = await res.json();
    const hourly = json?.hourly;

    if (!hourly?.time?.length) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const idx = pickNearestHourlyIndex(hourly.time);
    if (idx < 0) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const formatted = {
      airport: icao,
      timeUtc: hourly.time[idx] || null,
      levels: {
        '2000FT': formatWindAloft(hourly.wind_direction_925hPa?.[idx], hourly.wind_speed_925hPa?.[idx]),
        'FL050': formatWindAloft(hourly.wind_direction_850hPa?.[idx], hourly.wind_speed_850hPa?.[idx]),
        'FL100': formatWindAloft(hourly.wind_direction_700hPa?.[idx], hourly.wind_speed_700hPa?.[idx])
      }
    };

    setCached(cacheKey, formatted, WEATHER_CACHE_TTL_MS);
    return formatted;
  } catch {
    return null;
  }
}

function hasAnyWindsLine(windsAloft) {
  if (!windsAloft) return false;

  const dep = windsAloft.dep?.levels || {};
  const arr = windsAloft.arr?.levels || {};

  return Object.values(dep).some(Boolean) || Object.values(arr).some(Boolean);
}

function buildWindsLine(label, winds) {
  if (!winds?.levels) return null;

  const parts = [];
  if (winds.levels['2000FT']) parts.push(`2000FT ${winds.levels['2000FT']}`);
  if (winds.levels['FL050']) parts.push(`FL050 ${winds.levels['FL050']}`);
  if (winds.levels['FL100']) parts.push(`FL100 ${winds.levels['FL100']}`);

  if (!parts.length) return null;
  return `${label} ${parts.join('   ')}`;
}

function buildDispatchData(query, depWx, arrWx, altnWx, windsAloft) {
  const now = new Date();

  return {
    date: formatDateOnly(now),
    timeUtc: formatUtcClock(now),
    flight: normalizeFlight(query.flight),
    route: normalizeRoute(query.route),
    remarks: normalizeRemarks(query.remarks),
    dep: normalizeIcao(query.dep),
    arr: normalizeIcao(query.arr),
    depWx,
    arrWx,
    altnWx,
    windsAloft,
    includeDepMetar: parseBoolean(query.includeDepMetar),
    includeDepTaf: parseBoolean(query.includeDepTaf),
    includeArrMetar: parseBoolean(query.includeArrMetar),
    includeArrTaf: parseBoolean(query.includeArrTaf),
    includeAltnMetar: parseBoolean(query.includeAltnMetar),
    includeAltnTaf: parseBoolean(query.includeAltnTaf),
    includeWindsAloft: parseBoolean(query.includeWindsAloft)
  };
}

function buildWeatherSectionLines(label, wx, wantMetar, wantTaf) {
  if (!wx || (!wantMetar && !wantTaf)) return [];

  const lines = [];
  lines.push(`${label} (${wx.airport})`);

  if (wantMetar) {
    if (wx.metarFallback && wx.metarSource) {
      lines.push('METAR MODE FALLBACK');
      lines.push(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`);
    } else if (wx.metar && wx.metar !== 'NOT AVAILABLE') {
      lines.push('METAR MODE LIVE');
    } else {
      lines.push('METAR MODE NO DATA');
    }

    lines.push(wx.metar || 'NOT AVAILABLE');
    lines.push('');
  }

  if (wantTaf) {
    if (wx.tafFallback && wx.tafSource) {
      lines.push('TAF MODE FALLBACK');
      lines.push(`TAF SRC ${wx.tafSource}/${wx.tafDistanceNm}NM`);
    } else if (wx.taf && wx.taf !== 'NOT AVAILABLE') {
      lines.push('TAF MODE LIVE');
    } else {
      lines.push('TAF MODE NO DATA');
    }

    lines.push(wx.taf || 'NOT AVAILABLE');
    lines.push('');
  }

  return lines;
}

function buildDispatchText(data) {
  const headerLine1 = [
    'OPS NATGLOBE AVIATION',
    `DATE ${data.date}`,
    `UTC ${data.timeUtc.replace(' UTC', '')}`
  ].join('   ');

  const headerLine2 = [
    data.flight ? `FLT ${data.flight}` : null,
    data.dep ? `ORIG ${data.dep}` : null,
    data.arr ? `DEST ${data.arr}` : null,
    data.route ? `ROUTE ${data.route}` : null
  ].filter(Boolean).join('   ');

  const lines = [
    'ACARS WEATHER REPORT',
    '--------------------',
    headerLine1,
    headerLine2,
    ''
  ];

  lines.push(...buildWeatherSectionLines('DEP WX', data.depWx, data.includeDepMetar, data.includeDepTaf));
  lines.push(...buildWeatherSectionLines('ARR WX', data.arrWx, data.includeArrMetar, data.includeArrTaf));
  lines.push(...buildWeatherSectionLines('ALTN WX', data.altnWx, data.includeAltnMetar, data.includeAltnTaf));

  if (data.includeWindsAloft && hasAnyWindsLine(data.windsAloft)) {
    lines.push('WINDS ALOFT');

    const depLine = buildWindsLine('DEP', data.windsAloft?.dep);
    const arrLine = buildWindsLine('ARR', data.windsAloft?.arr);

    if (depLine) lines.push(depLine);
    if (arrLine) lines.push(arrLine);

    lines.push('');
  }

  lines.push(`RMK ${generateAutoRemark(data)}`);
  lines.push('');
  lines.push('END OF REPORT');

  return lines.join('\n');
}

function getSingleSourceStatus(text, fallback) {
  if (!text || text === 'NOT AVAILABLE') {
    return { mode: 'NO DATA', rank: 3 };
  }

  if (fallback) {
    return { mode: 'FALLBACK', rank: 2 };
  }

  return { mode: 'LIVE', rank: 1 };
}

function combineStatuses(statuses) {
  if (!statuses.length) {
    return { mode: 'NO DATA', label: 'NO DATA' };
  }

  let worst = statuses[0];
  for (const status of statuses) {
    if (status.rank > worst.rank) worst = status;
  }

  return {
    mode: worst.mode,
    label: worst.mode
  };
}

function summarizeAirportStatus(wx, icao, includeMetar, includeTaf) {
  if (!icao) {
    return {
      airport: '',
      mode: 'EMPTY',
      label: ''
    };
  }

  if (!wx) {
    return {
      airport: icao,
      mode: 'NO DATA',
      label: 'NO DATA'
    };
  }

  const statuses = [];

  if (includeMetar) {
    statuses.push(getSingleSourceStatus(wx.metar, wx.metarFallback));
  }

  if (includeTaf) {
    statuses.push(getSingleSourceStatus(wx.taf, wx.tafFallback));
  }

  if (!statuses.length) {
    if (wx.metar && wx.metar !== 'NOT AVAILABLE') {
      return {
        airport: icao,
        mode: wx.metarFallback ? 'FALLBACK' : 'LIVE',
        label: wx.metarFallback ? 'FALLBACK' : 'LIVE'
      };
    }

    return {
      airport: icao,
      mode: 'NO DATA',
      label: 'NO DATA'
    };
  }

  const combined = combineStatuses(statuses);

  return {
    airport: icao,
    mode: combined.mode,
    label: combined.label
  };
}

async function getSuggestedAlternate(arrIcao, currentAltnIcao = '') {
  const arr = normalizeIcao(arrIcao);
  const currentAltn = normalizeIcao(currentAltnIcao);

  if (!arr) return null;

  const arrWx = await getAirportWeather(arr);
  if (!arrivalNeedsAlternate(arrWx)) return null;

  const candidates = await getNearbyAirports(arr, SUGGESTED_ALTN_LIMIT);

  for (const candidate of candidates) {
    if (candidate.icao === currentAltn) continue;

    const wx = await getAirportWeather(candidate.icao);
    if (!wx || !wx.metar || wx.metar === 'NOT AVAILABLE') continue;
    if (arrivalNeedsAlternate(wx)) continue;

    return {
      airport: candidate.icao,
      distanceNm: candidate.distanceNm,
      weatherMode: wx.metarFallback ? 'FALLBACK' : 'LIVE'
    };
  }

  return null;
}

function buildPerformanceOutputSection(
  kind,
  airport,
  wx,
  runwayIdent,
  runwayHeading,
  weightLbs,
  runwayCondition,
  runwayLengthFt,
  runwaySlopePct,
  runwayElevationFt
) {
  const metar = wx?.metar && wx.metar !== 'NOT AVAILABLE' ? String(wx.metar) : '';
  const wind = parseWindDetailed(metar);
  const oatC = parseTemperatureC(metar);
  const qnhHpa = parseQnhHpa(metar);
  const qnhIn = hpaToInHg(qnhHpa);
  const elevationFt = Number.isFinite(runwayElevationFt)
    ? Math.round(runwayElevationFt)
    : (Number.isFinite(airport?.elevationFt) ? Math.round(airport.elevationFt) : null);
  const densityAltitudeFt = computeDensityAltitudeFt(elevationFt, oatC, qnhHpa);

  const components =
    wind && !wind.variable && Number.isFinite(wind.direction) && Number.isFinite(wind.speed) && Number.isFinite(runwayHeading)
      ? computeWindComponents(wind.direction, wind.speed, runwayHeading)
      : { headwindKt: null, crosswindKt: null };

  if (kind === 'takeoff') {
    return {
      runwayAndWeather: {
        apt: {
          icao: airport?.icao || '',
          name: airport?.name || ''
        },
        wind: wind
          ? {
              raw: wind.raw,
              direction: wind.direction,
              speedKt: wind.speed,
              gustKt: wind.gust,
              variable: wind.variable
            }
          : null,
        rwy: runwayIdent || '',
        hwXw: {
          headwindKt: components.headwindKt,
          crosswindKt: components.crosswindKt
        },
        toraFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
        oatC,
        hdgDeg: runwayHeading,
        qnhIn,
        qnhHpa,
        elevFt: elevationFt,
        rwyCond: runwayCondition || '',
        densityAltFt: densityAltitudeFt
      },
      inputs: {
        weightLbs: Number.isFinite(weightLbs) ? weightLbs : AIRCRAFT_PROFILE.maxTakeoffWeightLbs,
        flaps: AIRCRAFT_PROFILE.takeoff.defaultFlaps,
        powerSetting: AIRCRAFT_PROFILE.takeoff.powerSetting,
        ac: AIRCRAFT_PROFILE.takeoff.ac
      },
      outputs: {
        vrKias: null,
        vyKias: null,
        vxKias: null,
        accelCheckFt: null,
        rwyLimitFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
        limitCode: AIRCRAFT_PROFILE.takeoff.limitCode,
        takeoffDistanceRequiredFt: null,
        distance50FtFt: null,
        rotatePitch: AIRCRAFT_PROFILE.takeoff.rotatePitch,
        xwindLimitKt: AIRCRAFT_PROFILE.takeoff.xwindLimitKt
      },
      messages: ['NONE']
    };
  }

  return {
    airportAndWeather: {
      apt: {
        icao: airport?.icao || '',
        name: airport?.name || ''
      },
      wind: wind
        ? {
            raw: wind.raw,
            direction: wind.direction,
            speedKt: wind.speed,
            gustKt: wind.gust,
            variable: wind.variable
          }
        : null,
      rwy: runwayIdent || '',
      hwXw: {
        headwindKt: components.headwindKt,
        crosswindKt: components.crosswindKt
      },
      ldaFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
      oatC,
      hdgDeg: runwayHeading,
      qnhIn,
      qnhHpa,
      elevFt: elevationFt,
      rwyCond: runwayCondition || '',
      densityAltFt: densityAltitudeFt,
      rwySlopePct: Number.isFinite(runwaySlopePct) ? runwaySlopePct : null
    },
    inputs: {
      landingWeightLbs: Number.isFinite(weightLbs) ? weightLbs : AIRCRAFT_PROFILE.maxLandingWeightLbs,
      flaps: AIRCRAFT_PROFILE.landing.defaultFlaps,
      approachSpeedKias: null,
      ac: AIRCRAFT_PROFILE.landing.ac
    },
    outputs: {
      vrefKias: null,
      finalApproachSpeedKias: null,
      landingDistanceRequiredFt: null,
      distance50FtFt: null,
      rwyLimitFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
      limitCode: AIRCRAFT_PROFILE.landing.limitCode,
      braking: AIRCRAFT_PROFILE.landing.braking
    },
    goAround: {
      goAroundPower: AIRCRAFT_PROFILE.landing.goAroundPower,
      flapsUpSpeedKias: null
    }
  };
}

async function buildPerformanceData(query) {
  const dep = normalizeIcao(query.dep);
  const arr = normalizeIcao(query.arr);

  const takeoffRunwayIdent = String(query.takeoffRunway || query.depRunway || '').trim().toUpperCase();
  const landingRunwayIdent = String(query.landingRunway || query.arrRunway || '').trim().toUpperCase();

  const takeoffRunwayHeading = normalizeHeading(query.takeoffRunwayHeading ?? query.depRunwayHeading);
  const landingRunwayHeading = normalizeHeading(query.landingRunwayHeading ?? query.arrRunwayHeading);

  const takeoffWeightLbs = normalizeWeight(query.takeoffWeightLbs ?? query.weightLbs);
  const landingWeightLbs = normalizeWeight(query.landingWeightLbs ?? query.weightLbs);

  const takeoffRunwayCondition = String(query.takeoffRunwayCondition || '').trim().toUpperCase();
  const landingRunwayCondition = String(query.landingRunwayCondition || '').trim().toUpperCase();

  const takeoffRunwayLengthFt = normalizeWeight(query.takeoffRunwayLengthFt ?? query.toraFt);
  const landingRunwayLengthFt = normalizeWeight(query.landingRunwayLengthFt ?? query.ldaFt);

  const landingRunwaySlopePct = Number.isFinite(Number(query.landingRunwaySlopePct))
    ? Number(query.landingRunwaySlopePct)
    : null;

  const takeoffRunwayElevationFt = normalizeWeight(query.takeoffRunwayElevationFt ?? query.depRunwayElevationFt);
  const landingRunwayElevationFt = normalizeWeight(query.landingRunwayElevationFt ?? query.arrRunwayElevationFt);

  const [depAirport, arrAirport, depWx, arrWx] = await Promise.all([
    dep ? findAirportByIcao(dep) : null,
    arr ? findAirportByIcao(arr) : null,
    dep ? getAirportWeather(dep) : null,
    arr ? getAirportWeather(arr) : null
  ]);

  return {
    aircraft: AIRCRAFT_PROFILE,
    takeoff: buildPerformanceOutputSection(
      'takeoff',
      depAirport,
      depWx,
      takeoffRunwayIdent,
      takeoffRunwayHeading,
      takeoffWeightLbs,
      takeoffRunwayCondition,
      takeoffRunwayLengthFt,
      null,
      takeoffRunwayElevationFt
    ),
    landing: buildPerformanceOutputSection(
      'landing',
      arrAirport,
      arrWx,
      landingRunwayIdent,
      landingRunwayHeading,
      landingWeightLbs,
      landingRunwayCondition,
      landingRunwayLengthFt,
      landingRunwaySlopePct,
      landingRunwayElevationFt
    )
  };
}

async function dispatchToPdfBuffer(data) {
  const pdfDoc = await PDFDocument.create();
  const regularFont = await pdfDoc.embedFont(StandardFonts.Courier);
  const boldFont = await pdfDoc.embedFont(StandardFonts.CourierBold);

  const pageWidth = 595.28;
  const pageHeight = 841.89;
  const page = pdfDoc.addPage([pageWidth, pageHeight]);

  page.drawRectangle({
    x: 0,
    y: 0,
    width: pageWidth,
    height: pageHeight,
    color: rgb(1, 1, 1)
  });

  const black = rgb(0, 0, 0);
  const blockWidth = 311.81;
  const left = (pageWidth - blockWidth) / 2;
  const indent = 12;
  let y = pageHeight - 36;

  const drawLine = (text, size = 10, bold = false, x = left) => {
    y -= size;
    page.drawText(text, {
      x,
      y,
      size,
      font: bold ? boldFont : regularFont,
      color: black
    });
    y -= 3.5;
  };

  const drawWrappedText = (text, size = 9.2, x = left + indent) => {
    const wrapped = wrapText(text, 50);
    for (const line of wrapped) {
      y -= size;
      page.drawText(line, {
        x,
        y,
        size,
        font: regularFont,
        color: black
      });
      y -= 3;
    }
  };

  const drawWeatherSection = (title, wx, wantMetar, wantTaf) => {
    if (!wx || (!wantMetar && !wantTaf)) return;

    drawLine(`${title} (${wx.airport})`, 10.5, true);

    if (wantMetar) {
      if (wx.metarFallback && wx.metarSource) {
        drawLine('METAR MODE FALLBACK', 9.2, true);
        drawLine(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`, 9.2, true);
      } else if (wx.metar && wx.metar !== 'NOT AVAILABLE') {
        drawLine('METAR MODE LIVE', 9.2, true);
      } else {
        drawLine('METAR MODE NO DATA', 9.2, true);
      }

      drawWrappedText(wx.metar || 'NOT AVAILABLE');
      y -= 4;
    }

    if (wantTaf) {
      if (wx.tafFallback && wx.tafSource) {
        drawLine('TAF MODE FALLBACK', 9.2, true);
        drawLine(`TAF SRC ${wx.tafSource}/${wx.tafDistanceNm}NM`, 9.2, true);
      } else if (wx.taf && wx.taf !== 'NOT AVAILABLE') {
        drawLine('TAF MODE LIVE', 9.2, true);
      } else {
        drawLine('TAF MODE NO DATA', 9.2, true);
      }

      drawWrappedText(wx.taf || 'NOT AVAILABLE');
      y -= 4;
    }
  };

  const headerLine1 = [
    'OPS NATGLOBE AVIATION',
    `DATE ${data.date}`,
    `UTC ${data.timeUtc.replace(' UTC', '')}`
  ].join('   ');

  const headerLine2 = [
    data.flight ? `FLT ${data.flight}` : null,
    data.dep ? `ORIG ${data.dep}` : null,
    data.arr ? `DEST ${data.arr}` : null,
    data.route ? `ROUTE ${data.route}` : null
  ].filter(Boolean).join('   ');

  drawLine('ACARS WEATHER REPORT', 12, true);
  drawLine('--------------------', 11, true);
  drawLine(headerLine1, 9.5, false);
  drawLine(headerLine2, 9.5, false);
  y -= 4;

  drawWeatherSection('DEP WX', data.depWx, data.includeDepMetar, data.includeDepTaf);
  drawWeatherSection('ARR WX', data.arrWx, data.includeArrMetar, data.includeArrTaf);
  drawWeatherSection('ALTN WX', data.altnWx, data.includeAltnMetar, data.includeAltnTaf);

  if (data.includeWindsAloft && hasAnyWindsLine(data.windsAloft)) {
    drawLine('WINDS ALOFT', 10.2, true);

    const depLine = buildWindsLine('DEP', data.windsAloft?.dep);
    const arrLine = buildWindsLine('ARR', data.windsAloft?.arr);

    if (depLine) drawWrappedText(depLine, 9.2, left + indent);
    if (arrLine) drawWrappedText(arrLine, 9.2, left + indent);

    y -= 4;
  }

  drawLine(`RMK ${generateAutoRemark(data)}`, 9.5, true);
  y -= 2;

  drawLine('END OF REPORT', 10.5, true);

  return Buffer.from(await pdfDoc.save());
}

app.get('/api/airport-runways', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);
    if (!icao) {
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
      return res.json([]);
    }

    const runways = await getAirportRunways(icao);

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json(runways);
  } catch (err) {
    console.error(err);
    res.status(500).json([]);
  }
});

app.get('/api/dispatch-pdf', async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);
    const includeWindsAloft = parseBoolean(req.query.includeWindsAloft);

    const [depWx, arrWx, altnWx, depWinds, arrWinds] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null,
      includeWindsAloft && dep ? getWindsAloftForAirport(dep) : null,
      includeWindsAloft && arr ? getWindsAloftForAirport(arr) : null
    ]);

    const windsAloft = includeWindsAloft ? { dep: depWinds, arr: arrWinds } : null;
    const data = buildDispatchData(req.query, depWx, arrWx, altnWx, windsAloft);
    const pdf = await dispatchToPdfBuffer(data);

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'attachment; filename="ACARS_DISPATCH_REPORT.pdf"');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(pdf);
  } catch (err) {
    console.error(err);
    res.status(500).send('ERROR');
  }
});

app.get('/api/dispatch-text', async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);
    const includeWindsAloft = parseBoolean(req.query.includeWindsAloft);

    const [depWx, arrWx, altnWx, depWinds, arrWinds] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null,
      includeWindsAloft && dep ? getWindsAloftForAirport(dep) : null,
      includeWindsAloft && arr ? getWindsAloftForAirport(arr) : null
    ]);

    const windsAloft = includeWindsAloft ? { dep: depWinds, arr: arrWinds } : null;
    const data = buildDispatchData(req.query, depWx, arrWx, altnWx, windsAloft);
    const reportText = buildDispatchText(data);

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(reportText);
  } catch (err) {
    console.error(err);
    res.status(500).send('FAILED TO BUILD REPORT');
  }
});

app.get('/api/dispatch-status', async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);

    const includeDepMetar = parseBoolean(req.query.includeDepMetar);
    const includeDepTaf = parseBoolean(req.query.includeDepTaf);
    const includeArrMetar = parseBoolean(req.query.includeArrMetar);
    const includeArrTaf = parseBoolean(req.query.includeArrTaf);
    const includeAltnMetar = parseBoolean(req.query.includeAltnMetar);
    const includeAltnTaf = parseBoolean(req.query.includeAltnTaf);

    const [depWx, arrWx, altnWx] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null
    ]);

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      dep: summarizeAirportStatus(depWx, dep, includeDepMetar, includeDepTaf),
      arr: summarizeAirportStatus(arrWx, arr, includeArrMetar, includeArrTaf),
      altn: summarizeAirportStatus(altnWx, altn, includeAltnMetar, includeAltnTaf)
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      dep: { airport: '', mode: 'NO DATA', label: '' },
      arr: { airport: '', mode: 'NO DATA', label: '' },
      altn: { airport: '', mode: 'NO DATA', label: '' }
    });
  }
});

app.get('/api/dispatch-advice', async (req, res) => {
  try {
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);

    const arrWx = arr ? await getAirportWeather(arr) : null;
    const needsAlternate = arrivalNeedsAlternate(arrWx);
    const suggestedAlternate = needsAlternate ? await getSuggestedAlternate(arr, altn) : null;

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      arrivalNeedsAlternate: needsAlternate,
      suggestedAlternate
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      arrivalNeedsAlternate: false,
      suggestedAlternate: null
    });
  }
});

app.get('/api/performance', async (req, res) => {
  try {
    const data = await buildPerformanceData(req.query);

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      aircraft: AIRCRAFT_PROFILE,
      takeoff: {
        runwayAndWeather: {
          apt: { icao: '', name: '' },
          wind: null,
          rwy: '',
          hwXw: { headwindKt: null, crosswindKt: null },
          toraFt: null,
          oatC: null,
          hdgDeg: null,
          qnhIn: null,
          qnhHpa: null,
          elevFt: null,
          rwyCond: '',
          densityAltFt: null
        },
        inputs: {
          weightLbs: AIRCRAFT_PROFILE.maxTakeoffWeightLbs,
          flaps: AIRCRAFT_PROFILE.takeoff.defaultFlaps,
          powerSetting: AIRCRAFT_PROFILE.takeoff.powerSetting,
          ac: AIRCRAFT_PROFILE.takeoff.ac
        },
        outputs: {
          vrKias: null,
          vyKias: null,
          vxKias: null,
          accelCheckFt: null,
          rwyLimitFt: null,
          limitCode: AIRCRAFT_PROFILE.takeoff.limitCode,
          takeoffDistanceRequiredFt: null,
          distance50FtFt: null,
          rotatePitch: AIRCRAFT_PROFILE.takeoff.rotatePitch,
          xwindLimitKt: AIRCRAFT_PROFILE.takeoff.xwindLimitKt
        },
        messages: ['NONE']
      },
      landing: {
        airportAndWeather: {
          apt: { icao: '', name: '' },
          wind: null,
          rwy: '',
          hwXw: { headwindKt: null, crosswindKt: null },
          ldaFt: null,
          oatC: null,
          hdgDeg: null,
          qnhIn: null,
          qnhHpa: null,
          elevFt: null,
          rwyCond: '',
          densityAltFt: null,
          rwySlopePct: null
        },
        inputs: {
          landingWeightLbs: AIRCRAFT_PROFILE.maxLandingWeightLbs,
          flaps: AIRCRAFT_PROFILE.landing.defaultFlaps,
          approachSpeedKias: null,
          ac: AIRCRAFT_PROFILE.landing.ac
        },
        outputs: {
          vrefKias: null,
          finalApproachSpeedKias: null,
          landingDistanceRequiredFt: null,
          distance50FtFt: null,
          rwyLimitFt: null,
          limitCode: AIRCRAFT_PROFILE.landing.limitCode,
          braking: AIRCRAFT_PROFILE.landing.braking
        },
        goAround: {
          goAroundPower: AIRCRAFT_PROFILE.landing.goAroundPower,
          flapsUpSpeedKias: null
        }
      }
    });
  }
});

app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  try {
    await loadAirportDatabase();
    await loadRunwayDatabase();
  } catch (err) {
    console.warn('Airport/runway DB warm-up failed:', err.message);
  }
});

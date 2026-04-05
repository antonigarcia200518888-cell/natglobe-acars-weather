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

    if (vis !== null) {
      if (vis < 2000) {
        remarks.push('POOR VIS');
      } else if (vis < 5000) {
        remarks.push('LOW VIS');
      }
    }

    if (wind) {
      if (wind.gust && wind.gust >= 25) {
        remarks.push(`GUSTS ${wind.gust}KT`);
      } else if (wind.speed >= 20) {
        remarks.push('STRONG WIND');
      }
    }

    if (cloudBase !== null && cloudBase <= 2000) {
      remarks.push('LOW CEILING');
    }

    if (phenomena.includes('TS')) {
      remarks.push('TS');
    } else if (phenomena.includes('FG')) {
      remarks.push('FOG');
    } else if (
      phenomena.includes('RA') ||
      phenomena.includes('SHRA') ||
      phenomena.includes('DZ') ||
      phenomena.includes('SN') ||
      phenomena.includes('SHSN') ||
      phenomena.includes('FZRA')
    ) {
      remarks.push('PRECIP');
    }
  }

  inspect(data.depWx);
  inspect(data.arrWx);

  const unique = [...new Set(remarks)];
  return unique.length ? unique.join(' / ') : 'NIL';
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

  function pushAirportBlock(label, wx, wantMetar) {
    if (!wx || !wantMetar) return;

    lines.push(`${label} (${wx.airport})`);

    if (wx.metarFallback && wx.metarSource) {
      lines.push('MODE FALLBACK');
      lines.push(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`);
    }

    lines.push(wx.metar || 'NOT AVAILABLE');
    lines.push('');
  }

  pushAirportBlock('DEP WX', data.depWx, data.includeDepMetar);
  pushAirportBlock('ARR WX', data.arrWx, data.includeArrMetar);
  pushAirportBlock('ALTN WX', data.altnWx, data.includeAltnMetar);

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

  const drawAirportSection = (title, wx, wantMetar) => {
    if (!wx || !wantMetar) return;

    drawLine(`${title} (${wx.airport})`, 10.5, true);

    if (wx.metarFallback && wx.metarSource) {
      drawLine('MODE FALLBACK', 9.2, true);
      drawLine(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`, 9.2, true);
    }

    drawWrappedText(wx.metar || 'NOT AVAILABLE');
    y -= 4;
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

  drawAirportSection('DEP WX', data.depWx, data.includeDepMetar);
  drawAirportSection('ARR WX', data.arrWx, data.includeArrMetar);
  drawAirportSection('ALTN WX', data.altnWx, data.includeAltnMetar);

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

app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  try {
    await loadAirportDatabase();
  } catch (err) {
    console.warn('Airport DB warm-up failed:', err.message);
  }
});

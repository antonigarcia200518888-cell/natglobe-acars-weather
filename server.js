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
    lon
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
      lon: Number(a.lon)
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

function buildDispatchData(query, depWx, arrWx, altnWx) {
  const now = new Date();

  return {
    date: formatDateOnly(now),
    timeUtc: formatUtcClock(now),
    flight: normalizeFlight(query.flight),
    route: normalizeRoute(query.route),
    remarks: normalizeRemarks(query.remarks),
    depWx,
    arrWx,
    altnWx,
    includeDepMetar: parseBoolean(query.includeDepMetar),
    includeDepTaf: parseBoolean(query.includeDepTaf),
    includeArrMetar: parseBoolean(query.includeArrMetar),
    includeArrTaf: parseBoolean(query.includeArrTaf),
    includeAltnMetar: parseBoolean(query.includeAltnMetar),
    includeAltnTaf: parseBoolean(query.includeAltnTaf)
  };
}

function buildDispatchText(data) {
  const headerLine = [
    'OPS: NATGLOBE AVIATION',
    data.flight ? `FLT ${data.flight}` : null,
    data.route ? `ROUTE ${data.route}` : null,
    `DATE ${data.date}`,
    `UTC ${data.timeUtc.replace(' UTC', '')}`
  ].filter(Boolean).join('   ');

  const lines = [
    'ACARS WEATHER REPORT',
    '--------------------',
    headerLine,
    ''
  ];

  function pushAirportBlock(label, wx, wantMetar) {
    if (!wx || !wantMetar) return;

    lines.push(`${label} (${wx.airport})`);

    if (wx.metarFallback && wx.metarSource) {
      lines.push('MODE FALLBACK');
      lines.push(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`);
    }

    lines.push('METAR');
    lines.push(wx.metar || 'NOT AVAILABLE');
    lines.push('');
  }

  pushAirportBlock('DEP WX', data.depWx, data.includeDepMetar);
  pushAirportBlock('ARR WX', data.arrWx, data.includeArrMetar);
  pushAirportBlock('ALTN WX', data.altnWx, data.includeAltnMetar);

  if (data.remarks) {
    lines.push(`RMK ${data.remarks}`);
    lines.push('');
  }

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

    drawLine('METAR', 10, true);
    drawWrappedText(wx.metar || 'NOT AVAILABLE');
    y -= 4;
  };

  const headerLine = [
    'OPS: NATGLOBE AVIATION',
    data.flight ? `FLT ${data.flight}` : null,
    data.route ? `ROUTE ${data.route}` : null,
    `DATE ${data.date}`,
    `UTC ${data.timeUtc.replace(' UTC', '')}`
  ].filter(Boolean).join('   ');

  drawLine('ACARS WEATHER REPORT', 12, true);
  drawLine('--------------------', 11, true);
  drawLine(headerLine, 9.5, false);
  y -= 4;

  drawAirportSection('DEP WX', data.depWx, data.includeDepMetar);
  drawAirportSection('ARR WX', data.arrWx, data.includeArrMetar);
  drawAirportSection('ALTN WX', data.altnWx, data.includeAltnMetar);

  if (data.remarks) {
    drawLine(`RMK ${data.remarks}`, 9.5, true);
    y -= 2;
  }

  drawLine('END OF REPORT', 10.5, true);

  return Buffer.from(await pdfDoc.save());
}

app.get('/api/dispatch-pdf', async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);

    const [depWx, arrWx, altnWx] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null
    ]);

    const data = buildDispatchData(req.query, depWx, arrWx, altnWx);
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

    const [depWx, arrWx, altnWx] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null
    ]);

    const data = buildDispatchData(req.query, depWx, arrWx, altnWx);
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

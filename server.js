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

let airportDbCache = {
  data: null,
  loadedAt: 0,
  promise: null
};

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

  if (!/^[A-Z]{4}$/.test(icao)) return null;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  return {
    icao,
    name: String(row.name || '').trim(),
    municipality: String(row.municipality || '').trim(),
    country: String(row.iso_country || '').trim().toUpperCase(),
    type: String(row.type || '').trim(),
    lat,
    lon
  };
}

async function fetchCsvAirports(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'NatGlobeAviation/1.0' }
  });

  if (!res.ok) {
    throw new Error(`AIRPORT DB HTTP ${res.status} for ${url}`);
  }

  const csv = await res.text();
  const rows = parseCsv(csv);

  return rows
    .map(normalizeAirportRow)
    .filter(Boolean);
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
      const [fi, ee] = await Promise.all([
        fetchCsvAirports('https://ourairports.com/countries/FI/airports.csv'),
        fetchCsvAirports('https://ourairports.com/countries/EE/airports.csv')
      ]);

      const merged = [...fi, ...ee];
      const dedupedMap = new Map();

      for (const apt of merged) {
        if (!dedupedMap.has(apt.icao)) dedupedMap.set(apt.icao, apt);
      }

      const data = Array.from(dedupedMap.values());
      airportDbCache.data = data;
      airportDbCache.loadedAt = Date.now();
      airportDbCache.promise = null;

      console.log(`Loaded airport DB: ${data.length} FI+EE aerodromes`);
      return data;
    } catch (err) {
      console.warn('Failed to load remote FI+EE airport DB, using bundled fallback:', err.message);
      const data = getBundledAirportFallback();
      airportDbCache.data = data;
      airportDbCache.loadedAt = Date.now();
      airportDbCache.promise = null;
      return data;
    }
  })();

  return airportDbCache.promise;
}

async function getNearbyAirports(icao, limit = 20) {
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
  const res = await fetch(url, {
    headers: { 'User-Agent': 'NatGlobeAviation/1.0' }
  });

  if (res.status === 204) return null;
  if (!res.ok) return null;

  const text = await res.text();
  return String(text || '').trim() || null;
}

async function getMetar(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);
}

async function getTaf(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);
}

async function getFirstAvailable(fetchFn, requestedIcao) {
  const own = await fetchFn(requestedIcao);
  if (own) {
    return {
      text: own,
      source: requestedIcao,
      fallback: false,
      distanceKm: 0,
      distanceNm: 0
    };
  }

  const nearby = await getNearbyAirports(requestedIcao, 60);

  for (const apt of nearby) {
    const found = await fetchFn(apt.icao);
    if (found) {
      return {
        text: found,
        source: apt.icao,
        fallback: true,
        distanceKm: apt.distanceKm,
        distanceNm: apt.distanceNm
      };
    }
  }

  return {
    text: 'NOT AVAILABLE',
    source: null,
    fallback: false,
    distanceKm: null,
    distanceNm: null
  };
}

function stripHtml(html) {
  return String(html || '')
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<\/h\d>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&deg;/gi, '°')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{2,}/g, '\n')
    .trim();
}

function cleanLine(line) {
  return String(line || '').replace(/\s+/g, ' ').trim();
}

function extractLine(text, regex) {
  const match = text.match(regex);
  return match ? cleanLine(match[0]) : null;
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'NatGlobeAviation/1.0' }
  });

  if (!res.ok) return null;
  return await res.text();
}

function parseEfhvLocalWx(html) {
  const text = stripHtml(html);
  const lines = [];

  const sensorNotice = extractLine(text, /Anturista akku loppu[^\n]*/i);
  const measured = extractLine(text, /Mitattu\s+[^\n]+/i);

  if (sensorNotice) lines.push(sensorNotice);
  if (measured) lines.push(measured);

  const forecastSection = text.match(/Sääennuste([\s\S]{0,500})Tietoja kentästä/i);
  if (forecastSection) {
    const compact = cleanLine(forecastSection[1])
      .replace(/\s+/g, ' ')
      .slice(0, 180);
    if (compact) lines.push(`FORECAST SNAPSHOT: ${compact}`);
  }

  if (!lines.length) return null;

  return {
    sourceLabel: 'EFHV LOCAL PAGE',
    lines
  };
}

function parseEfnuLocalWx(html) {
  const text = stripHtml(html);
  const lines = [];

  const temp = extractLine(text, /Dew Point,\s*[-+0-9.,°C ]+/i);
  const hum = extractLine(text, /Humidity,\s*[-+0-9.,% ]+/i);
  const pressure = extractLine(text, /Barometer,\s*[-+0-9.,() mbar]+/i);
  const wind = extractLine(text, /Wind,\s*[-+0-9.,/A-Z°() msknot]+/i);
  const rain = extractLine(text, /Rain Rate,\s*[-+0-9.,/A-Z ]+/i);

  for (const item of [temp, hum, pressure, wind, rain]) {
    if (item) lines.push(item);
  }

  if (!lines.length) {
    const utcMarker = extractLine(text, /UTC TIME/i);
    if (utcMarker) lines.push('WX-INFO PAGE AVAILABLE');
  }

  if (!lines.length) return null;

  return {
    sourceLabel: 'EFNU LOCAL PAGE',
    lines
  };
}

async function getSupplementalLocalWx(icao) {
  try {
    if (icao === 'EFHV') {
      const html = await fetchHtml('https://efhv.fi/');
      if (!html) return null;
      return parseEfhvLocalWx(html);
    }

    if (icao === 'EFNU') {
      const html = await fetchHtml('https://efnu.fi/info-wx/');
      if (!html) return null;
      return parseEfnuLocalWx(html);
    }
  } catch (err) {
    console.warn(`Supplemental WX failed for ${icao}:`, err.message);
  }

  return null;
}

async function getAirportWeather(icao) {
  if (!icao) return null;

  const metarResult = await getFirstAvailable(getMetar, icao);
  const tafResult = await getFirstAvailable(getTaf, icao);

  let mode = 'LIVE';
  if (metarResult.fallback || tafResult.fallback) mode = 'FALLBACK';
  if (metarResult.text === 'NOT AVAILABLE' && tafResult.text === 'NOT AVAILABLE') mode = 'NO DATA';

  const localWx =
    metarResult.text === 'NOT AVAILABLE' && tafResult.text === 'NOT AVAILABLE'
      ? await getSupplementalLocalWx(icao)
      : null;

  return {
    airport: icao,
    mode,
    metar: metarResult.text,
    taf: tafResult.text,
    metarSource: metarResult.source,
    tafSource: tafResult.source,
    metarFallback: metarResult.fallback,
    tafFallback: tafResult.fallback,
    metarDistanceNm: metarResult.distanceNm,
    tafDistanceNm: tafResult.distanceNm,
    localWx
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

function formatLocalHelsinkiClock(d) {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Helsinki',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).format(d);
}

function buildDispatchData(query, depWx, arrWx, altnWx) {
  const now = new Date();

  return {
    date: formatDateOnly(now),
    timeUtc: formatUtcClock(now),
    timeLocal: formatLocalHelsinkiClock(now),
    flight: normalizeFlight(query.flight),
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
  const lines = [
    'ACARS WEATHER REPORT',
    '--------------------',
    'OPS SOURCE: NATGLOBE AVIATION',
    data.flight ? `FLIGHT: ${data.flight}` : null,
    `DATE: ${data.date}`,
    `TIME (UTC): ${data.timeUtc}`,
    `TIME (LOCAL): ${data.timeLocal}`,
    ''
  ].filter(Boolean);

  function pushAirportBlock(label, wx, wantMetar, wantTaf) {
    if (!wx) return;

    lines.push(`${label} (${wx.airport})`);
    lines.push(`MODE: ${wx.mode}`);

    if (wantMetar) {
      if (wx.metarFallback && wx.metarSource) {
        lines.push(`METAR SOURCE: ${wx.metarSource} / ${wx.metarDistanceNm} NM`);
      }
      lines.push('METAR:');
      lines.push(wx.metar || 'NOT AVAILABLE');
      lines.push('');
    }

    if (wantTaf) {
      if (wx.tafFallback && wx.tafSource) {
        lines.push(`TAF SOURCE: ${wx.tafSource} / ${wx.tafDistanceNm} NM`);
      }
      lines.push('TAF:');
      lines.push(wx.taf || 'NOT AVAILABLE');
      lines.push('');
    }

    if (wx.localWx) {
      lines.push(`LOCAL WX SOURCE: ${wx.localWx.sourceLabel}`);
      lines.push('LOCAL WX:');
      for (const item of wx.localWx.lines) {
        lines.push(item);
      }
      lines.push('');
    }
  }

  pushAirportBlock('DEP WEATHER', data.depWx, data.includeDepMetar, data.includeDepTaf);
  pushAirportBlock('ARR WEATHER', data.arrWx, data.includeArrMetar, data.includeArrTaf);
  pushAirportBlock('ALTN WEATHER', data.altnWx, data.includeAltnMetar, data.includeAltnTaf);

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

  const drawAirportSection = (title, wx, wantMetar, wantTaf) => {
    if (!wx) return;

    drawLine(`${title} (${wx.airport})`, 10.5, true);
    drawLine(`MODE: ${wx.mode}`, 9.5, true);

    if (wantMetar) {
      if (wx.metarFallback && wx.metarSource) {
        drawLine(`METAR SOURCE: ${wx.metarSource} / ${wx.metarDistanceNm} NM`, 8.8, true);
      }
      drawLine('METAR:', 10, true);
      drawWrappedText(wx.metar || 'NOT AVAILABLE');
      y -= 2;
    }

    if (wantTaf) {
      if (wx.tafFallback && wx.tafSource) {
        drawLine(`TAF SOURCE: ${wx.tafSource} / ${wx.tafDistanceNm} NM`, 8.8, true);
      }
      drawLine('TAF:', 10, true);
      drawWrappedText(wx.taf || 'NOT AVAILABLE');
      y -= 2;
    }

    if (wx.localWx) {
      drawLine(`LOCAL WX SOURCE: ${wx.localWx.sourceLabel}`, 8.8, true);
      drawLine('LOCAL WX:', 10, true);
      for (const item of wx.localWx.lines) {
        drawWrappedText(item);
      }
      y -= 2;
    }

    y -= 4;
  };

  drawLine('ACARS WEATHER REPORT', 12, true);
  drawLine('--------------------', 11, true);
  y -= 5;

  drawLine('OPS SOURCE: NATGLOBE AVIATION', 10, true);
  if (data.flight) drawLine(`FLIGHT: ${data.flight}`, 10, true);
  drawLine(`DATE: ${data.date}`, 10, true);
  drawLine(`TIME (UTC): ${data.timeUtc}`, 10, true);
  drawLine(`TIME (LOCAL): ${data.timeLocal}`, 10, true);
  y -= 4;

  drawAirportSection('DEP WEATHER', data.depWx, data.includeDepMetar, data.includeDepTaf);
  drawAirportSection('ARR WEATHER', data.arrWx, data.includeArrMetar, data.includeArrTaf);
  drawAirportSection('ALTN WEATHER', data.altnWx, data.includeAltnMetar, data.includeAltnTaf);

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

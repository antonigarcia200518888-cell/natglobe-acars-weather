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
const OURAIRPORTS_CSV_URL = 'https://davidmegginson.github.io/ourairports-data/airports.csv';

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

async function fetchCsvAirports(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'NatGlobeAviation/1.0' }
  });

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

async function getNearbyAirports(icao, limit = 60) {
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

async function getNearestOfficial(fetchFn, requestedIcao) {
  const nearby = await getNearbyAirports(requestedIcao, 80);

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

function pad2(n) {
  return String(n).padStart(2, '0');
}

function mapCloudFromOktas(oktas) {
  const n = Number(oktas);
  if (!Number.isFinite(n)) return '///';
  if (n <= 0) return 'SKC';
  if (n <= 2) return 'FEW020';
  if (n <= 4) return 'SCT020';
  if (n <= 7) return 'BKN020';
  return 'OVC020';
}

function buildPseudoMetar({
  icao,
  day,
  hour,
  minute,
  windDir,
  windSpd,
  windGust,
  temp,
  dew,
  pressure,
  cloud
}) {
  const dd = pad2(day ?? new Date().getUTCDate());
  const hh = pad2(hour ?? new Date().getUTCHours());
  const mm = pad2(minute ?? new Date().getUTCMinutes());

  const dir = Number.isFinite(Number(windDir))
    ? pad2(String(Math.round(Number(windDir) / 10) * 10).padStart(3, '0'))
    : '///';

  const spd = Number.isFinite(Number(windSpd))
    ? pad2(Math.round(Number(windSpd)))
    : '//';

  const gust =
    Number.isFinite(Number(windGust)) && Number(windGust) > Number(windSpd || 0)
      ? `G${pad2(Math.round(Number(windGust)))}`
      : '';

  const wind = dir === '///' ? 'VRB//KT' : `${String(dir).padStart(3, '0')}${spd}${gust}KT`;

  const t =
    Number.isFinite(Number(temp))
      ? (Number(temp) < 0 ? `M${pad2(Math.abs(Math.round(Number(temp))))}` : pad2(Math.round(Number(temp))))
      : '//';

  const d =
    Number.isFinite(Number(dew))
      ? (Number(dew) < 0 ? `M${pad2(Math.abs(Math.round(Number(dew))))}` : pad2(Math.round(Number(dew))))
      : '//';

  const q =
    Number.isFinite(Number(pressure))
      ? `Q${pad2(String(Math.round(Number(pressure))).padStart(4, '0'))}`
      : 'Q////';

  const cloudGroup = cloud || '///';

  return `${icao} ${dd}${hh}${mm}Z AUTO ${wind} 9999 ${cloudGroup} ${t}/${d} ${q}`;
}

function parseEfhvLocalMetar(html, icao) {
  const text = stripHtml(html);

  if (/Anturista akku loppu/i.test(text)) {
    return null;
  }

  const measuredMatch = text.match(/Mitattu\s+(\d{1,2})\.(\d{1,2})\.\s+(\d{1,2}):(\d{2})/i);
  const forecastLineMatch = text.match(
    /(\d{1,2}):(\d{2})\s+(-?\d+)°\s*(\d+)%\,\s*(-?\d+)°\s*(\d{1,3})°\s*(\d+)\((\d+)\)\s*(\d)\/8/i
  );

  if (!measuredMatch || !forecastLineMatch) {
    return null;
  }

  const day = Number(measuredMatch[1]);
  const hour = Number(measuredMatch[3]);
  const minute = Number(measuredMatch[4]);

  const temp = Number(forecastLineMatch[3]);
  const dew = Number(forecastLineMatch[5]);
  const windDir = Number(forecastLineMatch[6]);
  const windSpd = Number(forecastLineMatch[7]);
  const windGust = Number(forecastLineMatch[8]);
  const oktas = Number(forecastLineMatch[9]);

  return buildPseudoMetar({
    icao,
    day,
    hour,
    minute,
    windDir,
    windSpd,
    windGust,
    temp,
    dew,
    pressure: null,
    cloud: mapCloudFromOktas(oktas)
  });
}

function parseEfnuLocalMetar(html, icao) {
  const text = stripHtml(html);

  const compact = cleanLine(text);

  const timeMatch = compact.match(/UTC TIME\s+(\d{1,2}):(\d{2})/i);
  const windMatch = compact.match(/(?:WIND|TUULI)\s+(\d{1,3})\D+(\d{1,2})(?:\D+(\d{1,2}))?/i);
  const tempMatch = compact.match(/(?:TEMP|LÄMPÖ|T)\s+(-?\d+(?:[.,]\d+)?)/i);
  const dewMatch = compact.match(/(?:DEW|DEW POINT|DP)\s+(-?\d+(?:[.,]\d+)?)/i);
  const qnhMatch = compact.match(/(?:QNH|BAROMETER|PRESSURE)\s+(\d{3,4}(?:[.,]\d+)?)/i);
  const cloudMatch = compact.match(/(?:PILVET|CLOUD)\s+(\d)\/8/i);

  if (!timeMatch) return null;

  const now = new Date();
  const day = now.getUTCDate();
  const hour = Number(timeMatch[1]);
  const minute = Number(timeMatch[2]);

  return buildPseudoMetar({
    icao,
    day,
    hour,
    minute,
    windDir: windMatch ? Number(windMatch[1]) : null,
    windSpd: windMatch ? Number(windMatch[2]) : null,
    windGust: windMatch && windMatch[3] ? Number(windMatch[3]) : null,
    temp: tempMatch ? Number(String(tempMatch[1]).replace(',', '.')) : null,
    dew: dewMatch ? Number(String(dewMatch[1]).replace(',', '.')) : null,
    pressure: qnhMatch ? Number(String(qnhMatch[1]).replace(',', '.')) : null,
    cloud: cloudMatch ? mapCloudFromOktas(Number(cloudMatch[1])) : '///'
  });
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'NatGlobeAviation/1.0' }
  });

  if (!res.ok) return null;
  return await res.text();
}

async function getLocalGeneratedMetar(icao) {
  try {
    if (icao === 'EFHV') {
      const html = await fetchHtml('https://efhv.fi/');
      if (!html) return null;
      return parseEfhvLocalMetar(html, icao);
    }

    if (icao === 'EFNU') {
      const html = await fetchHtml('https://efnu.fi/info-wx/');
      if (!html) return null;
      return parseEfnuLocalMetar(html, icao);
    }
  } catch (err) {
    console.warn(`Local METAR build failed for ${icao}:`, err.message);
  }

  return null;
}

async function getAirportWeather(icao) {
  if (!icao) return null;

  const officialMetar = await getMetar(icao);
  const officialTaf = await getTaf(icao);

  let metar = officialMetar;
  let taf = officialTaf;
  let metarSource = officialMetar ? icao : null;
  let tafSource = officialTaf ? icao : null;
  let metarFallback = false;
  let tafFallback = false;
  let metarDistanceNm = 0;
  let tafDistanceNm = 0;
  let mode = 'LIVE';

  if (!metar) {
    const localGeneratedMetar = await getLocalGeneratedMetar(icao);
    if (localGeneratedMetar) {
      metar = localGeneratedMetar;
      mode = 'LOCAL';
      metarSource = null;
      metarFallback = false;
      metarDistanceNm = null;
    }
  }

  if (!metar) {
    const fallbackMetar = await getNearestOfficial(getMetar, icao);
    metar = fallbackMetar.text;
    metarSource = fallbackMetar.source;
    metarFallback = fallbackMetar.fallback;
    metarDistanceNm = fallbackMetar.distanceNm;
    if (fallbackMetar.fallback) mode = 'FALLBACK';
  }

  if (!taf) {
    const fallbackTaf = await getNearestOfficial(getTaf, icao);
    taf = fallbackTaf.text;
    tafSource = fallbackTaf.source;
    tafFallback = fallbackTaf.fallback;
    tafDistanceNm = fallbackTaf.distanceNm;
    if (fallbackTaf.fallback && mode !== 'LOCAL') mode = 'FALLBACK';
  }

  if (!metar) metar = 'NOT AVAILABLE';
  if (!taf) taf = 'NOT AVAILABLE';

  if (metar === 'NOT AVAILABLE' && taf === 'NOT AVAILABLE') {
    mode = 'NO DATA';
  }

  if (officialMetar && !officialTaf && tafFallback) {
    mode = 'FALLBACK';
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

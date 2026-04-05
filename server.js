import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import { airports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

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
    .replace(/\s+/g, ' ')
    .slice(0, 80);
}

function normalizeRemarks(input) {
  return String(input || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, 180);
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

function getNearbyAirports(icao, limit = 8) {
  const target = airports.find(a => a.icao === icao);

  if (target) {
    return airports
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

  return airports
    .filter(a => a.icao !== icao)
    .slice(0, limit)
    .map(a => ({
      ...a,
      distanceKm: null,
      distanceNm: null
    }));
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

  const nearby = getNearbyAirports(requestedIcao, 10);
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

async function getAirportWeather(icao) {
  if (!icao) return null;

  const metarResult = await getFirstAvailable(getMetar, icao);
  const tafResult = await getFirstAvailable(getTaf, icao);

  let mode = 'LIVE';
  if (metarResult.fallback || tafResult.fallback) mode = 'FALLBACK';
  if (metarResult.text === 'NOT AVAILABLE' && tafResult.text === 'NOT AVAILABLE') mode = 'NO DATA';

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
    tafDistanceNm: tafResult.distanceNm
  };
}

function wrapText(text, maxChars = 38) {
  const raw = String(text || '').trim();
  if (!raw) return [''];

  const words = raw.split(/\s+/).filter(Boolean);
  const lines = [];
  let current = '';

  for (const word of words) {
    if (word.length > maxChars) {
      if (current) {
        lines.push(current);
        current = '';
      }
      for (let i = 0; i < word.length; i += maxChars) {
        lines.push(word.slice(i, i + maxChars));
      }
      continue;
    }

    const candidate = current ? `${current} ${word}` : word;
    if (candidate.length <= maxChars) {
      current = candidate;
    } else {
      if (current) lines.push(current);
      current = word;
    }
  }

  if (current) lines.push(current);
  return lines.length ? lines : [''];
}

function formatUtcDisplay(d) {
  const y = d.getUTCFullYear();
  const mo = String(d.getUTCMonth() + 1).padStart(2, '0');
  const da = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${y}-${mo}-${da} ${hh}:${mm} UTC`;
}

function formatLocalHelsinkiTime(d) {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Helsinki',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).format(d).replace(',', '') + ' HEL';
}

function formatHeaderDateZ(d) {
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${dd}${hh}${mm}Z`;
}

function padLabel(label, width = 11) {
  return `${label}:`.padEnd(width, ' ');
}

function buildDispatchData(query, depWx, arrWx, altnWx) {
  const now = new Date();
  const dep = normalizeIcao(query.dep);
  const arr = normalizeIcao(query.arr);
  const altn = normalizeIcao(query.altn);

  return {
    reportId: formatHeaderDateZ(now),
    timeUtc: formatUtcDisplay(now),
    timeLocal: formatLocalHelsinkiTime(now),
    flight: normalizeFlight(query.flight),
    route: normalizeRoute(query.route || [dep, arr].filter(Boolean).join('/')),
    remarks: normalizeRemarks(query.remarks),
    dep,
    arr,
    altn,
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

function pushWrappedBlock(lines, label, value, wrapAt = 40) {
  const wrapped = wrapText(value, wrapAt);
  if (!wrapped.length) {
    lines.push(`${padLabel(label)} `);
    return;
  }

  lines.push(`${padLabel(label)} ${wrapped[0]}`);
  for (let i = 1; i < wrapped.length; i++) {
    lines.push(`${' '.repeat(11)} ${wrapped[i]}`);
  }
}

function buildWeatherSection(lines, title, wx, wantMetar, wantTaf) {
  if (!wx) return;

  lines.push('----------------------------------------');
  lines.push(`${title} ${wx.airport}`);
  lines.push(`${padLabel('STATUS')} ${wx.mode}`);

  if (wantMetar) {
    if (wx.metarFallback && wx.metarSource) {
      lines.push(`${padLabel('METAR SRC')} ${wx.metarSource} ${wx.metarDistanceNm}NM`);
    }
    pushWrappedBlock(lines, 'METAR', wx.metar || 'NOT AVAILABLE');
  }

  if (wantTaf) {
    if (wx.tafFallback && wx.tafSource) {
      lines.push(`${padLabel('TAF SRC')} ${wx.tafSource} ${wx.tafDistanceNm}NM`);
    }
    pushWrappedBlock(lines, 'TAF', wx.taf || 'NOT AVAILABLE');
  }
}

function buildDispatchText(data) {
  const lines = [];

  lines.push('NATGLOBE AVIATION ACARS DISPATCH');
  lines.push('========================================');
  lines.push(`${padLabel('DOC TYPE')} WX DISPATCH`);
  lines.push(`${padLabel('MSG REF')} ${data.reportId}`);
  if (data.flight) lines.push(`${padLabel('FLIGHT')} ${data.flight}`);
  if (data.route) lines.push(`${padLabel('ROUTE')} ${data.route}`);
  lines.push(`${padLabel('TIME UTC')} ${data.timeUtc}`);
  lines.push(`${padLabel('TIME HEL')} ${data.timeLocal}`);

  if (data.remarks) {
    lines.push('----------------------------------------');
    pushWrappedBlock(lines, 'RMKS', data.remarks);
  }

  buildWeatherSection(lines, 'DEP WEATHER', data.depWx, data.includeDepMetar, data.includeDepTaf);
  buildWeatherSection(lines, 'ARR WEATHER', data.arrWx, data.includeArrMetar, data.includeArrTaf);
  buildWeatherSection(lines, 'ALTN WX', data.altnWx, data.includeAltnMetar, data.includeAltnTaf);

  lines.push('----------------------------------------');
  lines.push('NOTAMS: EXTERNAL VIA FINTRAFFIC PIB');
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
  const stripWidth = 311.81;
  const left = (pageWidth - stripWidth) / 2;
  const indent = 12;
  const rightWrapChars = 40;
  let y = pageHeight - 34;

  const ensureRoom = (needed = 24) => {
    if (y < 50 + needed) {
      y = pageHeight - 34;
      pdfDoc.addPage([pageWidth, pageHeight]);
    }
  };

  const drawLine = (text, size = 9.6, bold = false, x = left) => {
    ensureRoom(size + 8);
    y -= size;
    page.drawText(text, {
      x,
      y,
      size,
      font: bold ? boldFont : regularFont,
      color: black
    });
    y -= 3.2;
  };

  const drawWrappedField = (label, value, size = 9.2) => {
    const wrapped = wrapText(value, rightWrapChars);
    drawLine(`${padLabel(label)} ${wrapped[0] || ''}`, size, false);
    for (let i = 1; i < wrapped.length; i++) {
      drawLine(`${' '.repeat(11)} ${wrapped[i]}`, size, false);
    }
  };

  const drawDivider = () => drawLine('----------------------------------------', 9.4, false);

  const drawWeatherSection = (title, wx, wantMetar, wantTaf) => {
    if (!wx) return;

    drawDivider();
    drawLine(`${title} ${wx.airport}`, 10, true);
    drawLine(`${padLabel('STATUS')} ${wx.mode}`, 9.2, true);

    if (wantMetar) {
      if (wx.metarFallback && wx.metarSource) {
        drawLine(`${padLabel('METAR SRC')} ${wx.metarSource} ${wx.metarDistanceNm}NM`, 8.8, true);
      }
      drawWrappedField('METAR', wx.metar || 'NOT AVAILABLE');
    }

    if (wantTaf) {
      if (wx.tafFallback && wx.tafSource) {
        drawLine(`${padLabel('TAF SRC')} ${wx.tafSource} ${wx.tafDistanceNm}NM`, 8.8, true);
      }
      drawWrappedField('TAF', wx.taf || 'NOT AVAILABLE');
    }
  };

  drawLine('NATGLOBE AVIATION ACARS DISPATCH', 11.2, true);
  drawLine('========================================', 10.2, true);
  drawLine(`${padLabel('DOC TYPE')} WX DISPATCH`, 9.4, true);
  drawLine(`${padLabel('MSG REF')} ${data.reportId}`, 9.4, true);
  if (data.flight) drawLine(`${padLabel('FLIGHT')} ${data.flight}`, 9.4, true);
  if (data.route) drawWrappedField('ROUTE', data.route, 9.2);
  drawLine(`${padLabel('TIME UTC')} ${data.timeUtc}`, 9.2, true);
  drawLine(`${padLabel('TIME HEL')} ${data.timeLocal}`, 9.2, true);

  if (data.remarks) {
    drawDivider();
    drawWrappedField('RMKS', data.remarks, 9.2);
  }

  drawWeatherSection('DEP WEATHER', data.depWx, data.includeDepMetar, data.includeDepTaf);
  drawWeatherSection('ARR WEATHER', data.arrWx, data.includeArrMetar, data.includeArrTaf);
  drawWeatherSection('ALTN WX', data.altnWx, data.includeAltnMetar, data.includeAltnTaf);

  drawDivider();
  drawLine('NOTAMS: EXTERNAL VIA FINTRAFFIC PIB', 9.2, true);
  drawLine('END OF REPORT', 9.8, true);

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

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

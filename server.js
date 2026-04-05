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
        distanceNm: apt.distanceNm
      };
    }
  }

  return {
    text: 'NOT AVAILABLE',
    source: null,
    fallback: false,
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
      current = word;
    }
  }

  if (current) lines.push(current);
  return lines.length ? lines : [''];
}

function parseBoolean(v) {
  return String(v).toLowerCase() === 'true';
}

function buildNotamStub(airportsList) {
  const filtered = airportsList.filter(Boolean);
  if (!filtered.length) return 'NOTAMS NOT REQUESTED';
  return [
    'NOTAM FETCHER NOT CONNECTED YET',
    `REQUESTED FOR: ${filtered.join(', ')}`,
    'ADD OFFICIAL EUROPEAN AIS / PIB SOURCE NEXT'
  ].join('\n');
}

function buildDispatchData(query, depWx, arrWx, altnWx) {
  return {
    timeUtc: new Date().toUTCString(),
    flight: String(query.flight || '').trim().toUpperCase(),
    dep: depWx,
    arr: arrWx,
    altn: altnWx,
    includeDepMetar: parseBoolean(query.includeDepMetar),
    includeDepTaf: parseBoolean(query.includeDepTaf),
    includeArrMetar: parseBoolean(query.includeArrMetar),
    includeArrTaf: parseBoolean(query.includeArrTaf),
    includeAltnMetar: parseBoolean(query.includeAltnMetar),
    includeAltnTaf: parseBoolean(query.includeAltnTaf),
    includeNotams: parseBoolean(query.includeNotams),
    notamText: buildNotamStub([
      depWx?.airport,
      arrWx?.airport,
      altnWx?.airport
    ])
  };
}

function buildDispatchText(data) {
  const lines = [
    'ACARS WEATHER REPORT',
    '--------------------',
    'OPS SOURCE: NATGLOBE AVIATION',
    data.flight ? `FLIGHT: ${data.flight}` : null,
    `TIME (UTC): ${data.timeUtc}`
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
    }

    if (wantTaf) {
      if (wx.tafFallback && wx.tafSource) {
        lines.push(`TAF SOURCE: ${wx.tafSource} / ${wx.tafDistanceNm} NM`);
      }
      lines.push('TAF:');
      lines.push(wx.taf || 'NOT AVAILABLE');
    }
  }

  pushAirportBlock('DEP WEATHER', data.dep, data.includeDepMetar, data.includeDepTaf);
  pushAirportBlock('ARR WEATHER', data.arr, data.includeArrMetar, data.includeArrTaf);
  pushAirportBlock('ALTN WEATHER', data.altn, data.includeAltnMetar, data.includeAltnTaf);

  if (data.includeNotams) {
    lines.push('NOTAMS');
    lines.push(data.notamText);
  }

  lines.push('END OF REPORT');
  return lines.join('\n');
}

async function dispatchToPdfBuffer(data) {
  const pdfDoc = await PDFDocument.create();
  const regularFont = await pdfDoc.embedFont(StandardFonts.Courier);
  const boldFont = await pdfDoc.embedFont(StandardFonts.CourierBold);

  // A4 portrait for maximum preview compatibility
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

  // 110 mm content block centered on page
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

  const drawWrapped = (text, size = 9.2, x = left + indent) => {
    const lines = wrapText(text, 50);
    for (const line of lines) {
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

  drawLine('ACARS WEATHER REPORT', 12, true);
  drawLine('--------------------', 11, true);
  y -= 5;

  drawLine('OPS SOURCE: NATGLOBE AVIATION', 10, true);
  if (data.flight) drawLine(`FLIGHT: ${data.flight}`, 10, true);
  drawLine(`TIME (UTC): ${data.timeUtc}`, 10, true);
  y -= 4;

  const drawAirportSection = (title, wx, wantMetar, wantTaf) => {
    if (!wx) return;

    drawLine(`${title} (${wx.airport})`, 10.5, true);
    drawLine(`MODE: ${wx.mode}`, 9.5, true);

    if (wantMetar) {
      if (wx.metarFallback && wx.metarSource) {
        drawLine(`METAR SOURCE: ${wx.metarSource} / ${wx.metarDistanceNm} NM`, 8.8, true);
      }
      drawLine('METAR:', 10, true);
      drawWrapped(wx.metar || 'NOT AVAILABLE');
    }

    if (wantTaf) {
      if (wx.tafFallback && wx.tafSource) {
        drawLine(`TAF SOURCE: ${wx.tafSource} / ${wx.tafDistanceNm} NM`, 8.8, true);
      }
      drawLine('TAF:', 10, true);
      drawWrapped(wx.taf || 'NOT AVAILABLE');
    }

    y -= 4;
  };

  drawAirportSection('DEP WEATHER', data.dep, data.includeDepMetar, data.includeDepTaf);
  drawAirportSection('ARR WEATHER', data.arr, data.includeArrMetar, data.includeArrTaf);
  drawAirportSection('ALTN WEATHER', data.altn, data.includeAltnMetar, data.includeAltnTaf);

  if (data.includeNotams) {
    drawLine('NOTAMS', 10.5, true);
    drawWrapped(data.notamText);
    y -= 4;
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

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

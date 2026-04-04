import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createCanvas, registerFont } from 'canvas';
import { PDFDocument, rgb } from 'pdf-lib';
import { airports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

registerFont(path.join(__dirname, 'cour.ttf'), {
  family: 'CourierNewEmbedded'
});

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

async function getWeatherWithFallback(icao) {
  const metarResult = await getFirstAvailable(getMetar, icao);
  const tafResult = await getFirstAvailable(getTaf, icao);

  let mode = 'LIVE';
  if (metarResult.fallback || tafResult.fallback) mode = 'FALLBACK';
  if (metarResult.text === 'NOT AVAILABLE' && tafResult.text === 'NOT AVAILABLE') mode = 'NO DATA';

  return {
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

function buildReportData(icao, weather) {
  return {
    airport: icao,
    timeUtc: new Date().toUTCString(),
    metar: weather.metar || 'NOT AVAILABLE',
    taf: weather.taf || 'NOT AVAILABLE',
    mode: weather.mode,
    metarSource: weather.metarSource,
    tafSource: weather.tafSource,
    metarFallback: weather.metarFallback,
    tafFallback: weather.tafFallback,
    metarDistanceNm: weather.metarDistanceNm,
    tafDistanceNm: weather.tafDistanceNm
  };
}

function buildReportText(report) {
  return [
    'ACARS WEATHER REPORT',
    '--------------------',
    'OPS SOURCE: NATGLOBE AVIATION',
    `TIME (UTC): ${report.timeUtc}`,
    `REQUESTED AIRPORT: ${report.airport}`,
    `MODE: ${report.mode}`,
    report.metarFallback && report.metarSource
      ? `METAR SOURCE: ${report.metarSource} / ${report.metarDistanceNm} NM`
      : null,
    report.tafFallback && report.tafSource
      ? `TAF SOURCE: ${report.tafSource} / ${report.tafDistanceNm} NM`
      : null,
    'METAR:',
    report.metar,
    'TAF:',
    report.taf,
    'END OF REPORT'
  ].filter(Boolean).join('\n');
}

function drawBoldText(ctx, text, x, y) {
  ctx.fillText(text, x, y);
  ctx.fillText(text, x + 0.4, y);
}

function reportToPngBuffer(report) {
  const canvasWidth = 1200;
  const canvasHeight = 2200;

  const ctxCanvas = createCanvas(canvasWidth, canvasHeight);
  const ctx = ctxCanvas.getContext('2d', { alpha: false });

  ctx.fillStyle = '#FFFFFF';
  ctx.fillRect(0, 0, canvasWidth, canvasHeight);

  ctx.fillStyle = '#000000';
  ctx.textBaseline = 'top';
  ctx.antialias = 'gray';
  ctx.patternQuality = 'best';
  ctx.quality = 'best';
  ctx.textDrawingMode = 'glyph';

  const left = 42;
  const indent = 28;
  let y = 34;

  const lineGap = 12;

  const drawLine = (text, size = 34, bold = false, x = left) => {
    ctx.font = `${size}px "CourierNewEmbedded"`;
    if (bold) {
      drawBoldText(ctx, text, x, y);
    } else {
      ctx.fillText(text, x, y);
    }
    y += size + lineGap;
  };

  const drawWrapped = (lines, size = 28, x = left + indent) => {
    ctx.font = `${size}px "CourierNewEmbedded"`;
    for (const line of lines) {
      ctx.fillText(line, x, y);
      y += size + 10;
    }
  };

  drawLine('ACARS WEATHER REPORT', 34, true);
  drawLine('--------------------', 30, true);
  y += 8;

  drawLine('OPS SOURCE: NATGLOBE AVIATION', 28, true);
  drawLine(`TIME (UTC): ${report.timeUtc}`, 28, true);
  drawLine(`REQUESTED AIRPORT: ${report.airport}`, 28, true);
  drawLine(`MODE: ${report.mode}`, 28, true);

  if (report.metarFallback && report.metarSource) {
    drawLine(`METAR SOURCE: ${report.metarSource} / ${report.metarDistanceNm} NM`, 24, true);
  }

  if (report.tafFallback && report.tafSource) {
    drawLine(`TAF SOURCE: ${report.tafSource} / ${report.tafDistanceNm} NM`, 24, true);
  }

  y += 10;

  drawLine('METAR:', 30, true);
  drawWrapped(wrapText(report.metar, 36), 27);

  y += 10;

  drawLine('TAF:', 30, true);
  drawWrapped(wrapText(report.taf, 36), 27);

  y += 14;
  drawLine('END OF REPORT', 30, true);

  return ctxCanvas.toBuffer('image/png');
}

async function reportToPdfBuffer(report) {
  const pdfDoc = await PDFDocument.create();

  const pngBuffer = reportToPngBuffer(report);
  const pngImage = await pdfDoc.embedPng(pngBuffer);

  // 110 mm x 210 mm fixed page for better thermal preview compatibility
  const pageWidth = 311.81;
  const pageHeight = 595.28;

  const page = pdfDoc.addPage([pageWidth, pageHeight]);

  page.drawRectangle({
    x: 0,
    y: 0,
    width: pageWidth,
    height: pageHeight,
    color: rgb(1, 1, 1)
  });

  const imgDims = pngImage.scaleToFit(pageWidth - 8, pageHeight - 8);

  page.drawImage(pngImage, {
    x: 4,
    y: pageHeight - imgDims.height - 4,
    width: imgDims.width,
    height: imgDims.height
  });

  return Buffer.from(await pdfDoc.save());
}

app.get('/api/weather-pdf', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);
    if (!icao) return res.status(400).send('INVALID ICAO');

    const weather = await getWeatherWithFallback(icao);
    const report = buildReportData(icao, weather);
    const pdf = await reportToPdfBuffer(report);

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${icao}_ACARS_WEATHER.pdf"`);
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(pdf);
  } catch (err) {
    console.error(err);
    res.status(500).send('ERROR');
  }
});

app.get('/api/report-text', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);
    if (!icao) return res.status(400).send('INVALID ICAO');

    const weather = await getWeatherWithFallback(icao);
    const report = buildReportData(icao, weather);
    const reportText = buildReportText(report);

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

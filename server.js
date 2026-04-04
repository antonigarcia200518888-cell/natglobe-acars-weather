import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { PDFDocument, rgb } from 'pdf-lib';
import fontkit from '@pdf-lib/fontkit';
import { airports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const courierFontBytes = fs.readFileSync(path.join(__dirname, 'cour.ttf'));

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

function wrapText(text, maxChars = 58) {
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
    `TIME (UTC): ${report.timeUtc}`,
    `AIRPORT: ${report.airport}`,
    `MODE: ${report.mode}`,
    report.metarFallback && report.metarSource
      ? `METAR SOURCE: ${report.metarSource} (nearest available, ${report.metarDistanceNm} NM)`
      : null,
    report.tafFallback && report.tafSource
      ? `TAF SOURCE: ${report.tafSource} (nearest available, ${report.tafDistanceNm} NM)`
      : null,
    'METAR:',
    report.metar,
    'TAF:',
    report.taf,
    'END OF REPORT'
  ].filter(Boolean).join('\n');
}

async function reportToPdfBuffer(report) {
  const pdfDoc = await PDFDocument.create();
  pdfDoc.registerFontkit(fontkit);

  const courierFont = await pdfDoc.embedFont(courierFontBytes);

  // A4 landscape
  const page = pdfDoc.addPage([842, 595]);
  const { width, height } = page.getSize();

  page.drawRectangle({
    x: 0,
    y: 0,
    width,
    height,
    color: rgb(1, 1, 1)
  });

  const black = rgb(0, 0, 0);

  let y = height - 42;
  const left = 36;
  const wrapIndent = 18;

  const drawLine = (text, size = 18) => {
    page.drawText(text, {
      x: left,
      y,
      size,
      font: courierFont,
      color: black
    });
    y -= size + 6;
  };

  const drawIndentedLines = (lines, size = 17) => {
    for (const line of lines) {
      page.drawText(line, {
        x: left + wrapIndent,
        y,
        size,
        font: courierFont,
        color: black
      });
      y -= size + 5;
    }
  };

  drawLine('ACARS WEATHER REPORT', 20);
  drawLine('--------------------', 18);
  y -= 6;

  drawLine(`TIME (UTC): ${report.timeUtc}`, 17);
  drawLine(`AIRPORT: ${report.airport}`, 17);
  drawLine(`MODE: ${report.mode}`, 17);

  if (report.metarFallback && report.metarSource) {
    drawLine(
      `METAR SOURCE: ${report.metarSource} (nearest available, ${report.metarDistanceNm} NM)`,
      15
    );
  }

  if (report.tafFallback && report.tafSource) {
    drawLine(
      `TAF SOURCE: ${report.tafSource} (nearest available, ${report.tafDistanceNm} NM)`,
      15
    );
  }

  y -= 8;

  drawLine('METAR:', 18);
  drawIndentedLines(wrapText(report.metar, 62), 17);

  y -= 8;

  drawLine('TAF:', 18);
  drawIndentedLines(wrapText(report.taf, 62), 17);

  y -= 10;
  drawLine('END OF REPORT', 18);

  return Buffer.from(await pdfDoc.save());
}

app.get('/api/weather-pdf', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);

    if (!icao) {
      return res.status(400).json({ error: 'Invalid ICAO' });
    }

    const weather = await getWeatherWithFallback(icao);
    const report = buildReportData(icao, weather);
    const pdfBuffer = await reportToPdfBuffer(report);

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${icao}_ACARS_WEATHER.pdf"`);
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(pdfBuffer);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to generate weather PDF' });
  }
});

app.get('/api/report-text', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);

    if (!icao) {
      return res.status(400).send('INVALID ICAO');
    }

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

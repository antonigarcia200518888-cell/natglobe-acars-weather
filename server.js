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

async function reportToPdfBuffer(report) {
  const pdfDoc = await PDFDocument.create();

  const regularFont = await pdfDoc.embedFont(StandardFonts.Courier);
  const boldFont = await pdfDoc.embedFont(StandardFonts.CourierBold);

  // 110 mm paper width
  const pageWidth = 311.81;
  const left = 10;
  const indent = 8;
  const topMargin = 12;
  const bottomMargin = 12;

  const metarLines = wrapText(report.metar, 34);
  const tafLines = wrapText(report.taf, 34);

  const items = [
    { text: 'ACARS WEATHER REPORT', size: 10, bold: true, indent: 0 },
    { text: '--------------------', size: 9, bold: true, indent: 0 },
    { blank: 4 },

    { text: 'OPS SOURCE: NATGLOBE AVIATION', size: 8.5, bold: true, indent: 0 },
    { text: `TIME (UTC): ${report.timeUtc}`, size: 8.5, bold: true, indent: 0 },
    { text: `REQUESTED AIRPORT: ${report.airport}`, size: 8.5, bold: true, indent: 0 },
    { text: `MODE: ${report.mode}`, size: 8.5, bold: true, indent: 0 },

    ...(report.metarFallback && report.metarSource
      ? [{ text: `METAR SOURCE: ${report.metarSource} / ${report.metarDistanceNm} NM`, size: 7.8, bold: true, indent: 0 }]
      : []),

    ...(report.tafFallback && report.tafSource
      ? [{ text: `TAF SOURCE: ${report.tafSource} / ${report.tafDistanceNm} NM`, size: 7.8, bold: true, indent: 0 }]
      : []),

    { blank: 4 },

    { text: 'METAR:', size: 9, bold: true, indent: 0 },
    ...metarLines.map(line => ({ text: line, size: 8.4, bold: false, indent: indent })),

    { blank: 4 },

    { text: 'TAF:', size: 9, bold: true, indent: 0 },
    ...tafLines.map(line => ({ text: line, size: 8.4, bold: false, indent: indent })),

    { blank: 4 },
    { text: 'END OF REPORT', size: 9, bold: true, indent: 0 }
  ];

  let contentHeight = topMargin;
  for (const item of items) {
    if (item.blank) {
      contentHeight += item.blank;
    } else {
      contentHeight += item.size + 2.5;
    }
  }
  contentHeight += bottomMargin;

  const pageHeight = Math.max(140, contentHeight);
  const page = pdfDoc.addPage([pageWidth, pageHeight]);

  page.drawRectangle({
    x: 0,
    y: 0,
    width: pageWidth,
    height: pageHeight,
    color: rgb(1, 1, 1)
  });

  const black = rgb(0, 0, 0);
  let y = pageHeight - topMargin;

  for (const item of items) {
    if (item.blank) {
      y -= item.blank;
      continue;
    }

    y -= item.size;

    page.drawText(item.text, {
      x: left + (item.indent || 0),
      y,
      size: item.size,
      font: item.bold ? boldFont : regularFont,
      color: black
    });

    y -= 2.5;
  }

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

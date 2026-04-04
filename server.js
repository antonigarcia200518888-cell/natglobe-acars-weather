import express from 'express';
import sharp from 'sharp';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { airports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Load uploaded Courier font from repo root
const courierFontBase64 = fs.readFileSync(path.join(__dirname, 'cour.ttf')).toString('base64');

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

function wrapText(text, maxChars = 56) {
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

function buildReportText(icao, weather) {
  const metarSourceLine =
    weather.metarFallback && weather.metarSource
      ? `METAR SOURCE: ${weather.metarSource} (nearest available, ${weather.metarDistanceNm} NM)`
      : null;

  const tafSourceLine =
    weather.tafFallback && weather.tafSource
      ? `TAF SOURCE: ${weather.tafSource} (nearest available, ${weather.tafDistanceNm} NM)`
      : null;

  return [
    'ACARS WEATHER REPORT',
    '--------------------',
    `TIME (UTC): ${new Date().toUTCString()}`,
    `AIRPORT: ${icao}`,
    `MODE: ${weather.mode}`,
    metarSourceLine,
    tafSourceLine,
    'METAR:',
    weather.metar || 'NOT AVAILABLE',
    'TAF:',
    weather.taf || 'NOT AVAILABLE',
    'END OF REPORT'
  ].filter(Boolean).join('\n');
}

function escapeXml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

async function reportToPngBuffer(report) {
  const pageWidth = 1600;
  const left = 70;
  const top = 60;
  const lineGap = 30;
  const sectionGap = 12;
  const wrapIndent = 34;

  const metarLines = wrapText(report.metar, 58);
  const tafLines = wrapText(report.taf, 58);

  const lines = [
    { text: 'ACARS WEATHER REPORT', bold: true, size: 24, x: left },
    { text: '--------------------', bold: true, size: 22, x: left },
    { blank: 10 },

    { text: `TIME (UTC): ${report.timeUtc}`, bold: true, size: 22, x: left },
    { text: `AIRPORT: ${report.airport}`, bold: true, size: 22, x: left },

    ...(report.mode !== 'LIVE'
      ? [{ text: `MODE: ${report.mode}`, bold: true, size: 20, x: left }]
      : []),

    ...(report.metarFallback && report.metarSource
      ? [{
          text: `METAR SOURCE: ${report.metarSource} (nearest available, ${report.metarDistanceNm} NM)`,
          bold: true,
          size: 18,
          x: left
        }]
      : []),

    ...(report.tafFallback && report.tafSource
      ? [{
          text: `TAF SOURCE: ${report.tafSource} (nearest available, ${report.tafDistanceNm} NM)`,
          bold: true,
          size: 18,
          x: left
        }]
      : []),

    { blank: 14 },

    { text: 'METAR:', bold: true, size: 22, x: left },
    ...metarLines.map(line => ({ text: line, bold: false, size: 21, x: left + wrapIndent })),

    { blank: 14 },

    { text: 'TAF:', bold: true, size: 22, x: left },
    ...tafLines.map(line => ({ text: line, bold: false, size: 21, x: left + wrapIndent })),

    { blank: 18 },
    { text: 'END OF REPORT', bold: true, size: 22, x: left }
  ];

  let y = top;
  for (const item of lines) {
    if (item.blank) y += item.blank;
    else y += lineGap;
  }

  const pageHeight = Math.max(900, y + 60);

  let currentY = top;
  const textElements = [];

  for (const item of lines) {
    if (item.blank) {
      currentY += item.blank;
      continue;
    }

    textElements.push(`
      <text
        x="${item.x}"
        y="${currentY}"
        font-family="CourierNewEmbedded"
        font-size="${item.size}"
        fill="#000000"
        font-weight="${item.bold ? '700' : '400'}"
      >${escapeXml(item.text)}</text>
    `);

    currentY += lineGap;
  }

  const svg = `
  <svg width="${pageWidth}" height="${pageHeight}" xmlns="http://www.w3.org/2000/svg">
    <style>
      @font-face {
        font-family: 'CourierNewEmbedded';
        src: url(data:font/ttf;base64,${courierFontBase64}) format('truetype');
      }
      text {
        text-rendering: geometricPrecision;
        shape-rendering: geometricPrecision;
      }
    </style>
    <rect x="0" y="0" width="${pageWidth}" height="${pageHeight}" fill="#FFFFFF" />
    ${textElements.join('\n')}
  </svg>`;

  return sharp(Buffer.from(svg))
    .flatten({ background: '#FFFFFF' })
    .png({ compressionLevel: 0, quality: 100 })
    .toBuffer();
}

app.get('/api/weather-v2', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);

    if (!icao) {
      return res.status(400).json({ error: 'Invalid ICAO' });
    }

    const weather = await getWeatherWithFallback(icao);

    const report = {
      airport: icao,
      timeUtc: new Date().toUTCString(),
      metar: weather.metar,
      taf: weather.taf,
      mode: weather.mode,
      metarSource: weather.metarSource,
      tafSource: weather.tafSource,
      metarFallback: weather.metarFallback,
      tafFallback: weather.tafFallback,
      metarDistanceNm: weather.metarDistanceNm,
      tafDistanceNm: weather.tafDistanceNm
    };

    const png = await reportToPngBuffer(report);

    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Content-Disposition', `attachment; filename="${icao}_ACARS_WEATHER.png"`);
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(png);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to generate weather report' });
  }
});

app.get('/api/report-text', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);

    if (!icao) {
      return res.status(400).send('INVALID ICAO');
    }

    const weather = await getWeatherWithFallback(icao);
    const reportText = buildReportText(icao, weather);

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

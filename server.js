import express from 'express';
import sharp from 'sharp';
import path from 'path';
import { fileURLToPath } from 'url';
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
      .map(a => ({
        ...a,
        distanceKm: haversineKm(target, a),
        distanceNm: formatNm(haversineKm(target, a))
      }))
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

function escapeXml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
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

async function textToPng(report) {
  const pageWidth = 1600;
  const pageMinHeight = 900;
  const left = 70;
  const top = 60;
  const lineGap = 28;
  const sectionGap = 10;
  const wrapIndent = 34;

  const metarLines = wrapText(report.metar, 58);
  const tafLines = wrapText(report.taf, 58);

  let y = top;
  const parts = [];

  function addText(x, text, options = {}) {
    const { bold = false, size = 23 } = options;
    parts.push(
      `<text x="${x}" y="${y}" font-family="Courier New, Courier, monospace" font-size="${size}" fill="#000000" font-weight="${bold ? '700' : '400'}">${escapeXml(text)}</text>`
    );
    y += lineGap;
  }

  function addBlank(space = sectionGap) {
    y += space;
  }

  addText(left, 'ACARS WEATHER REPORT', { bold: true, size: 24 });
  addText(left, '--------------------', { bold: true, size: 22 });
  addBlank(10);

  addText(left, `TIME (UTC): ${report.timeUtc}`, { bold: true, size: 22 });
  addText(left, `AIRPORT: ${report.airport}`, { bold: true, size: 22 });

  if (report.mode !== 'LIVE') {
    addText(left, `MODE: ${report.mode}`, { bold: true, size: 20 });
  }

  if (report.metarFallback && report.metarSource) {
    addText(
      left,
      `METAR SOURCE: ${report.metarSource} (nearest available, ${report.metarDistanceNm} NM)`,
      { bold: true, size: 18 }
    );
  }

  if (report.tafFallback && report.tafSource) {
    addText(
      left,
      `TAF SOURCE: ${report.tafSource} (nearest available, ${report.tafDistanceNm} NM)`,
      { bold: true, size: 18 }
    );
  }

  addBlank(14);

  addText(left, 'METAR:', { bold: true, size: 22 });
  metarLines.forEach((line) => {
    addText(left + wrapIndent, line, { size: 21 });
  });

  addBlank(14);

  addText(left, 'TAF:', { bold: true, size: 22 });
  tafLines.forEach((line) => {
    addText(left + wrapIndent, line, { size: 21 });
  });

  addBlank(18);
  addText(left, 'END OF REPORT', { bold: true, size: 22 });

  const pageHeight = Math.max(pageMinHeight, y + 60);

  const svg = `
<svg width="${pageWidth}" height="${pageHeight}" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="#FFFFFF"/>
  ${parts.join('\n')}
</svg>`;

  return sharp(Buffer.from(svg))
    .png({ compressionLevel: 0, quality: 100 })
    .sharpen()
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

    const png = await textToPng(report);

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

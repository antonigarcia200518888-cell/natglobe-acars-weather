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

function getNearbyAirports(icao, limit = 8) {
  const target = airports.find(a => a.icao === icao);

  // If target airport exists in airports.js, use true nearest fallback.
  if (target) {
    return airports
      .filter(a => a.icao !== icao)
      .map(a => ({ ...a, distanceKm: haversineKm(target, a) }))
      .sort((a, b) => a.distanceKm - b.distanceKm)
      .slice(0, limit);
  }

  // If target airport is NOT in airports.js, still try fallback by scanning the known list.
  // This is not true "nearest", but it prevents the fallback from failing completely.
  return airports
    .filter(a => a.icao !== icao)
    .slice(0, limit)
    .map(a => ({ ...a, distanceKm: null }));
}

async function fetchRaw(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'NatGlobeAviation/1.0 ACARS Weather Tool'
    }
  });

  if (res.status === 204) return null;
  if (!res.ok) return null;

  const text = await res.text();
  const cleaned = String(text || '').trim();
  return cleaned || null;
}

async function getMetar(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);
}

async function getTaf(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);
}

async function getFirstAvailable(productFn, requestedIcao) {
  const own = await productFn(requestedIcao);
  if (own) {
    return {
      text: own,
      source: requestedIcao,
      fallback: false
    };
  }

  const nearby = getNearbyAirports(requestedIcao, 10);

  for (const apt of nearby) {
    const candidate = await productFn(apt.icao);
    if (candidate) {
      return {
        text: candidate,
        source: apt.icao,
        fallback: true,
        distanceKm: apt.distanceKm
      };
    }
  }

  return {
    text: 'NOT AVAILABLE',
    source: null,
    fallback: false,
    distanceKm: null
  };
}

async function getWeatherWithFallback(icao) {
  const metarResult = await getFirstAvailable(getMetar, icao);
  const tafResult = await getFirstAvailable(getTaf, icao);

  let mode = 'LIVE';
  if (metarResult.fallback || tafResult.fallback) {
    mode = 'FALLBACK';
  }
  if (metarResult.text === 'NOT AVAILABLE' && tafResult.text === 'NOT AVAILABLE') {
    mode = 'NO DATA';
  }

  return {
    mode,
    metar: metarResult.text,
    taf: tafResult.text,
    metarSource: metarResult.source,
    tafSource: tafResult.source,
    metarFallback: metarResult.fallback,
    tafFallback: tafResult.fallback,
    metarDistanceKm: metarResult.distanceKm,
    tafDistanceKm: tafResult.distanceKm
  };
}

function buildReport({ airport, timeUtc, metar, taf, mode, metarSource, tafSource, metarFallback, tafFallback }) {
  return {
    airport,
    timeUtc,
    metar,
    taf,
    mode,
    metarSource,
    tafSource,
    metarFallback,
    tafFallback
  };
}

function escapeXml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function wrapText(text, maxChars = 64) {
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

async function textToPng(report) {
  const pageWidth = 1600;
  const pageMinHeight = 950;
  const left = 70;
  const top = 70;
  const labelGap = 42;
  const lineGap = 38;
  const sectionGap = 24;
  const metarIndent = 40;
  const tafIndent = 40;

  const metarLines = wrapText(report.metar, 68);
  const tafLines = wrapText(report.taf, 68);

  let y = top;
  const parts = [];

  function addText(x, text, options = {}) {
    const {
      bold = false,
      size = 28
    } = options;

    parts.push(
      `<text x="${x}" y="${y}" font-family="Courier New, monospace" font-size="${size}" fill="#000000" font-weight="${bold ? '700' : '400'}">${escapeXml(text)}</text>`
    );
    y += lineGap;
  }

  function addBlank(space = sectionGap) {
    y += space;
  }

  addText(left, 'ACARS WEATHER REPORT', { bold: true, size: 34 });
  addText(left, '--------------------', { bold: true, size: 30 });
  addBlank(16);

  addText(left, `TIME (UTC): ${report.timeUtc}`, { bold: true, size: 30 });
  addText(left, `AIRPORT: ${report.airport}`, { bold: true, size: 30 });

  if (report.mode && report.mode !== 'LIVE') {
    addText(left, `MODE: ${report.mode}`, { bold: true, size: 28 });
  }

  if (report.metarFallback && report.metarSource) {
    addText(left, `METAR SOURCE: ${report.metarSource} (nearest available)`, { bold: true, size: 26 });
  }

  if (report.tafFallback && report.tafSource) {
    addText(left, `TAF SOURCE: ${report.tafSource} (nearest available)`, { bold: true, size: 26 });
  }

  addBlank(18);

  addText(left, 'METAR:', { bold: true, size: 30 });
  for (const line of metarLines) {
    addText(left + metarIndent, line, { bold: false, size: 28 });
  }

  addBlank(18);

  addText(left, 'TAF:', { bold: true, size: 30 });
  for (const line of tafLines) {
    addText(left + tafIndent, line, { bold: false, size: 28 });
  }

  addBlank(24);
  addText(left, 'END OF REPORT', { bold: true, size: 30 });

  const pageHeight = Math.max(pageMinHeight, y + 80);

  const svg = `
<svg width="${pageWidth}" height="${pageHeight}" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="#FFFFFF"/>
  ${parts.join('\n')}
</svg>`;

  return sharp(Buffer.from(svg))
    .png({ compressionLevel: 0, quality: 100 })
    .toBuffer();
}

app.get('/api/weather', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);

    if (!icao) {
      return res.status(400).json({ error: 'Invalid ICAO' });
    }

    const weather = await getWeatherWithFallback(icao);

    const report = buildReport({
      airport: icao,
      timeUtc: new Date().toUTCString(),
      metar: weather.metar,
      taf: weather.taf,
      mode: weather.mode,
      metarSource: weather.metarSource,
      tafSource: weather.tafSource,
      metarFallback: weather.metarFallback,
      tafFallback: weather.tafFallback
    });

    const png = await textToPng(report);

    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Content-Disposition', `attachment; filename="${icao}_ACARS_WEATHER.png"`);
    res.send(png);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to generate weather report' });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

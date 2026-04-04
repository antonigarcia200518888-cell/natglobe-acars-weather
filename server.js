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
  return String(input || '').trim().toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
}

function haversineKm(a, b) {
  const R = 6371;
  const dLat = (b.lat - a.lat) * Math.PI / 180;
  const dLon = (b.lon - a.lon) * Math.PI / 180;
  const lat1 = a.lat * Math.PI / 180;
  const lat2 = b.lat * Math.PI / 180;

  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;

  return 2 * R * Math.asin(Math.sqrt(x));
}

function getNearbyAirports(icao, limit = 5) {
  const target = airports.find(a => a.icao === icao);
  if (!target) return [];

  return airports
    .filter(a => a.icao !== icao)
    .map(a => ({ ...a, distanceKm: haversineKm(target, a) }))
    .sort((a, b) => a.distanceKm - b.distanceKm)
    .slice(0, limit);
}

async function fetchRaw(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'NatGlobeAviation/1.0 ACARS Weather Tool'
    }
  });

  if (res.status === 204) return null;
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  const text = await res.text();
  return text.trim() || null;
}

async function getMetar(icao) {
  try {
    return await fetchRaw(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);
  } catch {
    return null;
  }
}

async function getTaf(icao) {
  try {
    return await fetchRaw(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);
  } catch {
    return null;
  }
}

async function getWeatherWithFallback(icao) {
  let metar = await getMetar(icao);
  let taf = await getTaf(icao);

  let metarSource = icao;
  let tafSource = icao;
  let mode = 'LIVE';

  if (!metar || !taf) {
    const nearby = getNearbyAirports(icao, 6);

    if (!metar) {
      for (const apt of nearby) {
        const candidate = await getMetar(apt.icao);
        if (candidate) {
          metar = candidate;
          metarSource = apt.icao;
          break;
        }
      }
    }

    if (!taf) {
      for (const apt of nearby) {
        const candidate = await getTaf(apt.icao);
        if (candidate) {
          taf = candidate;
          tafSource = apt.icao;
          break;
        }
      }
    }

    if (metarSource !== icao || tafSource !== icao) {
      mode = 'FALLBACK';
    }
  }

  return {
    mode,
    metar: metar || 'NOT AVAILABLE',
    taf: taf || 'NOT AVAILABLE',
    metarSource,
    tafSource
  };
}

function buildReport({ airport, timeUtc, metar, taf, mode, metarSource, tafSource }) {
  return {
    timeUtc,
    airport,
    metar,
    taf,
    mode,
    metarSource,
    tafSource
  };
}

function escapeXml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function wrapText(text, maxChars = 78) {
  const words = String(text || '').split(/\s+/);
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
  return lines;
}

async function textToPng(report) {
  const width = 1600;
  const height = 900;
  const left = 70;
  const top = 70;
  const lineHeight = 44;

  const metarLines = wrapText(report.metar, 78);
  const tafLines = wrapText(report.taf, 78);

  let y = top;
  const parts = [];

  function addLine(text, bold = false, size = 30) {
    parts.push(
      `<text x="${left}" y="${y}" font-family="Courier New, monospace" font-size="${size}" fill="#000" font-weight="${bold ? '700' : '400'}">${escapeXml(text)}</text>`
    );
    y += lineHeight;
  }

  addLine('ACARS WEATHER REPORT', true, 32);
  addLine('--------------------', true, 30);
  y += 12;

  addLine(`TIME (UTC): ${report.timeUtc}`, true, 30);
  addLine(`AIRPORT: ${report.airport}`, true, 30);

  if (report.mode && report.mode !== 'LIVE') {
    addLine(`MODE: ${report.mode}`, true, 30);
  }

  if (report.metarSource && report.metarSource !== report.airport) {
    addLine(`METAR SOURCE: ${report.metarSource} (nearest available)`, true, 28);
  }

  if (report.tafSource && report.tafSource !== report.airport) {
    addLine(`TAF SOURCE: ${report.tafSource} (nearest available)`, true, 28);
  }

  y += 18;

  addLine('METAR:', true, 30);
  for (const line of metarLines) addLine(line, false, 28);

  y += 18;

  addLine('TAF:', true, 30);
  for (const line of tafLines) addLine(line, false, 28);

  y += 24;
  addLine('END OF REPORT', true, 30);

  const svg = `
<svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="#f5f5f5"/>
  ${parts.join('\n')}
</svg>`;

  return sharp(Buffer.from(svg)).png().toBuffer();
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
      tafSource: weather.tafSource
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

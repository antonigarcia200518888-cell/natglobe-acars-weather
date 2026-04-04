import express from 'express';
import sharp from 'sharp';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { airports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const PORT = process.env.PORT || 3000;
const CACHE_DIR = path.join(__dirname, 'cache');
const USER_AGENT = 'NatGlobeAviation/1.0 ACARS Weather Tool';

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
  const x = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(x));
}

function getAirport(icao) {
  return airports.find(a => a.icao === icao) || null;
}

function getNearbyIcaos(icao, limit = 5) {
  const target = getAirport(icao);
  if (!target) return airports.filter(a => a.icao !== icao).slice(0, limit).map(a => a.icao);
  return airports
    .filter(a => a.icao !== icao)
    .map(a => ({ icao: a.icao, distanceKm: haversineKm(target, a) }))
    .sort((x, y) => x.distanceKm - y.distanceKm)
    .slice(0, limit)
    .map(a => a.icao);
}

async function fetchText(url) {
  const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT } });
  if (res.status === 204) return null;
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const text = (await res.text()).trim();
  return text || null;
}

async function fetchMetar(icao) {
  return fetchText(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);
}

async function fetchTaf(icao) {
  return fetchText(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);
}

async function findFirstAvailable(kind, icao, nearbyIcaos) {
  const fn = kind === 'METAR' ? fetchMetar : fetchTaf;
  try {
    const own = await fn(icao);
    if (own) return { requested: icao, source: icao, text: own, fallback: false };
  } catch {
    // continue to nearby/cached fallback later
  }

  for (const nearby of nearbyIcaos) {
    try {
      const value = await fn(nearby);
      if (value) return { requested: icao, source: nearby, text: value, fallback: true };
    } catch {
      // continue
    }
  }
  return { requested: icao, source: null, text: 'NOT AVAILABLE', fallback: false };
function center(text, width = 40) {
  const pad = Math.max(0, Math.floor((width - text.length) / 2));
  return " ".repeat(pad) + text;
}

function buildReport({ airport, timeUtc, metar, taf }) {
  return [
    center("ACARS WEATHER REPORT"),
    center("-------------------------"),
    "",
    center(`TIME (UTC): ${timeUtc}`),
    "",
    center(`AIRPORT: ${airport}`),
    "",
    "",
    center("METAR:"),
    metar || "NOT AVAILABLE",
    "",
    "",
    center("TAF:"),
    taf || "NOT AVAILABLE",
    "",
    "",
    center("END OF REPORT")
  ].join("\n");
}
function escapeXml(str) {
  return String(str)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

async function reportToPng(text) {
  const lines = text.split('\n');
  const width = 1400;
  const lineHeight = 34;
  const height = 80 + lines.length * lineHeight;
  const escaped = escapeXml(lines.join('\n'));
const svg = `
<svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="#f2f2f2"/>
  <text x="50%" y="60"
    text-anchor="middle"
    font-family="Courier New, monospace"
    font-size="24"
    fill="black"
    xml:space="preserve">${escapeXml(lines.join("\n"))}</text>
</svg>`;
  return sharp(Buffer.from(svg)).png().toBuffer();
}

async function saveLatest(icao, payload, pngBuffer) {
  await fs.mkdir(CACHE_DIR, { recursive: true });
  await fs.writeFile(path.join(CACHE_DIR, `${icao}.json`), JSON.stringify(payload, null, 2), 'utf8');
  await fs.writeFile(path.join(CACHE_DIR, `${icao}.png`), pngBuffer);
}

async function readLatest(icao) {
  try {
    const json = await fs.readFile(path.join(CACHE_DIR, `${icao}.json`), 'utf8');
    return JSON.parse(json);
  } catch {
    return null;
  }
}

async function generateReport(icao) {
  const nearby = getNearbyIcaos(icao, 5);
  const timeUtc = new Date().toUTCString();

  try {
    const [metar, taf] = await Promise.all([
      findFirstAvailable('METAR', icao, nearby),
      findFirstAvailable('TAF', icao, nearby)
    ]);

    const mode = (metar.text === 'NOT AVAILABLE' && taf.text === 'NOT AVAILABLE') ? 'LIVE - NO CURRENT DATA' : 'LIVE';
    const text = buildReport({ airport: icao, timeUtc, metar, taf, mode });
    const png = await reportToPng(text);
    const payload = { airport: icao, timeUtc, nearby, metar, taf, mode, text };
    await saveLatest(icao, payload, png);
    return { payload, png, fromCache: false };
  } catch (err) {
    const cached = await readLatest(icao);
    if (!cached) throw err;
    const text = buildReport({
      airport: icao,
      timeUtc: new Date().toUTCString(),
      metar: cached.metar,
      taf: cached.taf,
      mode: 'OFFLINE CACHE'
    });
    const png = await reportToPng(text);
    return {
      payload: {
        ...cached,
        timeUtc: new Date().toUTCString(),
        mode: 'OFFLINE CACHE',
        text
      },
      png,
      fromCache: true
    };
  }
}

app.get('/api/report-json', async (req, res) => {
  const icao = normalizeIcao(req.query.icao);
  if (!icao || icao.length !== 4) {
    return res.status(400).json({ error: 'Provide a valid 4-letter ICAO code.' });
  }

  try {
    const { payload, fromCache } = await generateReport(icao);
    res.json({ ...payload, fromCache });
  } catch (err) {
    res.status(503).json({ error: 'No live or cached report available.', details: err.message });
  }
});

app.get('/api/report-png', async (req, res) => {
  const icao = normalizeIcao(req.query.icao);
  if (!icao || icao.length !== 4) {
    return res.status(400).send('Provide a valid 4-letter ICAO code.');
  }

  try {
    const { png } = await generateReport(icao);
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Content-Disposition', `attachment; filename="${icao}_ACARS_WEATHER.png"`);
    res.send(png);
  } catch (err) {
    res.status(503).send('No live or cached report available.');
  }
});

app.get('/api/latest-png', async (req, res) => {
  const icao = normalizeIcao(req.query.icao);
  if (!icao || icao.length !== 4) {
    return res.status(400).send('Provide a valid 4-letter ICAO code.');
  }

  try {
    const png = await fs.readFile(path.join(CACHE_DIR, `${icao}.png`));
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Content-Disposition', `attachment; filename="${icao}_ACARS_WEATHER_LATEST.png"`);
    res.send(png);
  } catch {
    res.status(404).send('No cached PNG found for that ICAO.');
  }
});

app.listen(PORT, () => {
  console.log(`NatGlobe ACARS Weather app running at http://localhost:${PORT}`);
});

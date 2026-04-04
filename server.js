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

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

function normalizeIcao(input) {
  return String(input || '').trim().toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
}

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
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

async function textToPng(text) {
  const lines = text.split("\n");
  const width = 900;
  const lineHeight = 34;
  const height = 80 + lines.length * lineHeight;

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

async function fetchWeather(icao) {
  try {
    const res = await fetch(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);
    const metar = await res.text();

    const tafRes = await fetch(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);
    const taf = await tafRes.text();

    return {
      metar: metar.trim() || null,
      taf: taf.trim() || null
    };
  } catch (err) {
    return { metar: null, taf: null };
  }
}

app.get('/api/weather', async (req, res) => {
  const icao = normalizeIcao(req.query.icao);

  if (!icao) {
    return res.status(400).json({ error: 'Invalid ICAO' });
  }

  const { metar, taf } = await fetchWeather(icao);

  const timeUtc = new Date().toUTCString();

  const report = buildReport({
    airport: icao,
    timeUtc,
    metar,
    taf
  });

  const png = await textToPng(report);

  res.setHeader('Content-Type', 'image/png');
  res.send(png);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

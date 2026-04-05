// === YOUR ORIGINAL FILE WITH SAFE UPGRADES APPLIED ===

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import { airports as bundledAirports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const AIRPORT_DB_TTL_MS = 24 * 60 * 60 * 1000;
const WEATHER_CACHE_TTL_MS = 90 * 1000;
const NEGATIVE_CACHE_TTL_MS = 15 * 1000;

const FETCH_TIMEOUT_RAW_MS = 4500;
const FETCH_TIMEOUT_AIRPORT_DB_MS = 10000;

const FALLBACK_SEARCH_LIMIT = 16;
const FALLBACK_BATCH_SIZE = 4;

const OURAIRPORTS_CSV_URL = 'https://davidmegginson.github.io/ourairports-data/airports.csv';

let airportDbCache = { data: null, loadedAt: 0, promise: null };
const responseCache = new Map();

// ---------------- CACHE ----------------

function getCached(key) {
  const item = responseCache.get(key);
  if (!item) return undefined;
  if (Date.now() > item.expiresAt) {
    responseCache.delete(key);
    return undefined;
  }
  return item.value;
}

function setCached(key, value, ttlMs) {
  responseCache.set(key, {
    value,
    expiresAt: Date.now() + ttlMs
  });
}

// ---------------- NORMALIZERS ----------------

function normalizeIcao(input) {
  return String(input || '').trim().toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
}

function normalizeFlight(input) {
  return String(input || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 10);
}

function normalizeRoute(input) {
  return String(input || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9/ \-]/g, '')
    .replace(/\s+/g, ' ')
    .slice(0, 40);
}

function normalizeRemarks(input) {
  return String(input || '').trim().replace(/\s+/g, ' ').toUpperCase().slice(0, 80);
}

function parseBoolean(v) {
  return String(v).toLowerCase() === 'true';
}

// ---------------- GEO ----------------

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
  return Math.round(kmToNm(km));
}

// ---------------- FETCH ----------------

async function fetchWithTimeout(url, options = {}, timeoutMs = 5000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchRaw(url) {
  const cacheKey = `raw:${url}`;
  const cached = getCached(cacheKey);
  if (cached !== undefined) return cached;

  try {
    const res = await fetchWithTimeout(url, {}, FETCH_TIMEOUT_RAW_MS);

    if (!res.ok) return null;

    const text = String(await res.text() || '').trim() || null;
    setCached(cacheKey, text, WEATHER_CACHE_TTL_MS);
    return text;
  } catch {
    return null;
  }
}

const getMetar = (icao) =>
  fetchRaw(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);

const getTaf = (icao) =>
  fetchRaw(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);

// ---------------- PARSERS ----------------

function parseWindKt(metar) {
  const gustMatch = metar.match(/\b(\d{3}|VRB)(\d{2,3})G(\d{2,3})KT\b/);
  if (gustMatch) {
    return {
      speed: parseInt(gustMatch[2]),
      gust: parseInt(gustMatch[3])
    };
  }

  const basicMatch = metar.match(/\b(\d{3}|VRB)(\d{2,3})KT\b/);
  if (basicMatch) {
    return {
      speed: parseInt(basicMatch[2]),
      gust: null
    };
  }

  return null;
}

function parseVisibility(metar) {
  const m = metar.match(/\b(\d{4})\b/);
  return m ? parseInt(m[1]) : null;
}

function parseWeatherPhenomena(metar) {
  const tokens = ['FG', 'RA', 'SN', 'TS', 'DZ', 'SHRA', 'SHSN', 'FZRA'];
  return tokens.filter(t => metar.includes(t));
}

function parseCloudBase(metar) {
  const regex = /\b(FEW|SCT|BKN|OVC)(\d{3})\b/g;
  let match;
  let lowest = null;

  while ((match = regex.exec(metar))) {
    const base = parseInt(match[2]) * 100;
    if (!lowest || base < lowest) lowest = base;
  }

  return lowest;
}

// ---------------- 🔥 NEW RMK ----------------

function generateAutoRemark(data) {
  if (data.remarks) return data.remarks;

  const remarks = [];

  function inspect(wx) {
    if (!wx || !wx.metar || wx.metar === 'NOT AVAILABLE') return;

    const metar = wx.metar;

    const wind = parseWindKt(metar);
    const vis = parseVisibility(metar);
    const phenomena = parseWeatherPhenomena(metar);
    const cloud = parseCloudBase(metar);

    if (vis !== null) {
      if (vis < 2000) remarks.push('POOR VIS');
      else if (vis < 5000) remarks.push('LOW VIS');
    }

    if (wind) {
      if (wind.gust && wind.gust >= 25) remarks.push(`GUSTS ${wind.gust}KT`);
      else if (wind.speed >= 20) remarks.push('STRONG WIND');
    }

    if (cloud !== null && cloud <= 2000) {
      remarks.push('LOW CEILING');
    }

    if (phenomena.includes('TS')) remarks.push('TS');
    else if (phenomena.includes('FG')) remarks.push('FOG');
    else if (phenomena.length) remarks.push('PRECIP');
  }

  inspect(data.depWx);
  inspect(data.arrWx);

  return remarks.length ? [...new Set(remarks)].join(' / ') : 'NIL';
}

// ---------------- REPORT ----------------

function buildDispatchText(data) {
  return [
    'ACARS WEATHER REPORT',
    '--------------------',
    `OPS NATGLOBE AVIATION   DATE ${data.date}   UTC ${data.timeUtc.replace(' UTC', '')}`,
    `FLT ${data.flight}   ORIG ${data.dep}   DEST ${data.arr}   ROUTE ${data.route}`,
    '',
    `DEP WX (${data.dep})`,
    data.depWx.metar,
    '',
    `ARR WX (${data.arr})`,
    data.arrWx.metar,
    '',
    `RMK ${generateAutoRemark(data)}`,
    '',
    'END OF REPORT'
  ].join('\n');
}

// ---------------- SERVER ----------------

app.get('/api/dispatch-text', async (req, res) => {
  const depWx = { metar: await getMetar(req.query.dep) || 'NOT AVAILABLE' };
  const arrWx = { metar: await getMetar(req.query.arr) || 'NOT AVAILABLE' };

  const data = {
    date: new Date().toISOString().slice(0, 10),
    timeUtc: new Date().toISOString().slice(11, 16) + ' UTC',
    flight: normalizeFlight(req.query.flight),
    route: normalizeRoute(req.query.route),
    remarks: normalizeRemarks(req.query.remarks),
    dep: normalizeIcao(req.query.dep),
    arr: normalizeIcao(req.query.arr),
    depWx,
    arrWx
  };

  res.send(buildDispatchText(data));
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

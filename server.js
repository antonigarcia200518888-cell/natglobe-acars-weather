import express from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { createHmac, randomUUID, timingSafeEqual } from 'crypto';
import {
  generateAuthenticationOptions,
  generateRegistrationOptions,
  verifyAuthenticationResponse,
  verifyRegistrationResponse
} from '@simplewebauthn/server';
import { degrees, PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import fontkit from '@pdf-lib/fontkit';
import { airports as bundledAirports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const PILOT_ACCESS_CODE = process.env.PILOT_ACCESS_CODE || 'NATGLOBEOPS';
const PILOT_COOKIE_NAME = 'ng_pilot_access';
const PILOT_SESSION_SECRET = String(process.env.PILOT_SESSION_SECRET || PILOT_ACCESS_CODE || 'NATGLOBEOPS');
const PILOT_SESSION_MAX_AGE_SECONDS = 30 * 24 * 60 * 60;
const WALLET_ASSETS_DIR = path.join(__dirname, 'wallet-assets');
const PUBLIC_SITE_URL = String(process.env.PUBLIC_SITE_URL || 'https://ngaprivateaviation.com').replace(/\/$/, '');
const WEBAUTHN_RP_NAME = 'NGA Private Aviation Pilot Ops';
const WEBAUTHN_CHALLENGE_TTL_MS = 5 * 60 * 1000;

// The public domain opens the passenger experience. ACARS remains available to crew at /acars.
app.get('/', (req, res) => {
  res.redirect('/bookings');
});

app.get('/acars', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/vendor/simplewebauthn-browser.js', (req, res) => {
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.type('application/javascript');
  res.sendFile(path.join(__dirname, 'node_modules', '@simplewebauthn', 'browser', 'dist', 'bundle', 'index.umd.min.js'));
});

app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders(res, filePath) {
    if (filePath.endsWith('.woff2')) res.setHeader('Access-Control-Allow-Origin', '*');
  }
}));
app.use(express.json());

const AIRPORT_DB_TTL_MS = 24 * 60 * 60 * 1000;
const WEATHER_CACHE_TTL_MS = 90 * 1000;
const NEGATIVE_CACHE_TTL_MS = 15 * 1000;

const FETCH_TIMEOUT_RAW_MS = 4500;
const FETCH_TIMEOUT_AIRPORT_DB_MS = 10000;
const FETCH_TIMEOUT_WINDS_MS = 5500;

const FALLBACK_SEARCH_LIMIT = 16;
const FALLBACK_BATCH_SIZE = 4;
const SUGGESTED_ALTN_LIMIT = 12;

const OURAIRPORTS_CSV_URL = 'https://davidmegginson.github.io/ourairports-data/airports.csv';
const OURAIRPORTS_RUNWAYS_CSV_URL = 'https://davidmegginson.github.io/ourairports-data/runways.csv';
const OPERATING_COST_EUR_PER_HOUR = 300;
const PUBLIC_AIRCRAFT_TYPE = 'PA-28R';
const HANKO_SCENIC_EXPERIENCE = 'HANKO_REGATTA_SCENIC';
const HELSINKI_CITY_SCENIC_EXPERIENCE = 'HELSINKI_CITY_SCENIC';
const HELSINKI_DAY_VARIANT = 'DAY';
const HELSINKI_NIGHT_VARIANT = 'NIGHT';
const HANKO_SCENIC_DURATION_MIN = 30;
const HELSINKI_CITY_SCENIC_DURATION_MIN = 45;
const FIXED_BOOKING_ROUTE_PRICES = {
  'EFHV-EFHN': { oneWay: 129, roundtrip: 258, label: 'ESTIMATED HYVINKAA-HANKO COST SHARE' },
  'EFHN-EFHV': { oneWay: 129, roundtrip: 258, label: 'ESTIMATED HANKO-HYVINKAA COST SHARE' },
  'EFHK-EFHN': { oneWay: 499, roundtrip: 998, label: 'ESTIMATED HELSINKI-VANTAA-HANKO / AIRPORT FEES INCLUDED' },
  'EFHN-EFHK': { oneWay: 499, roundtrip: 998, label: 'ESTIMATED HANKO-HELSINKI-VANTAA / AIRPORT FEES INCLUDED' },
  'EFHN-EFHN': { oneWay: 30, roundtrip: 30, label: 'FIXED HANKO REGATTA SCENIC OVERFLIGHT' },
  'EFHV-EFHV': { oneWay: 125, roundtrip: 125, label: 'ESTIMATED HELSINKI CITY SCENIC OVERFLIGHT' },
  'EFNU-EFNU': { oneWay: 125, roundtrip: 125, label: 'ESTIMATED HELSINKI CITY SCENIC OVERFLIGHT' }
};

const costShareFlights = [
  {
    id: 'NG-CS-241',
    date: '2026-07-04',
    timeUtc: '0900Z',
    title: 'Helsinki Archipelago Scenic',
    dep: 'EFHV',
    arr: 'EFHV',
    route: 'EFHV - PORVOO - LOVIISA - SIPOO - EFHV',
    aircraft: PUBLIC_AIRCRAFT_TYPE,
    duration: '01H15',
    seatsTotal: 3,
    seatsAvailable: 2,
    costPerSeatEur: 95,
    status: 'OPEN',
    highlights: ['COASTAL VFR', 'WINDOW SEATS', 'PHOTO ORBITS IF WX ALLOWS'],
    notes: 'Shared-cost private flight. Final go/no-go depends on pilot, weather, aircraft status, and passenger fit.'
  },
  {
    id: 'NG-CS-242',
    date: '2026-07-11',
    timeUtc: '1000Z',
    title: 'Tallinn Lunch Hop',
    dep: 'EFHK',
    arr: 'EETN',
    route: 'EFHK - GULF OF FINLAND - EETN',
    aircraft: PUBLIC_AIRCRAFT_TYPE,
    duration: '00H55 EACH WAY',
    seatsTotal: 3,
    seatsAvailable: 3,
    costPerSeatEur: 145,
    status: 'INTEREST',
    highlights: ['CROSS-BORDER', 'PASSPORT REQUIRED', 'DAY RETURN'],
    notes: 'Expression of interest only until schedule, handling, customs, and weather are confirmed.'
  },
  {
    id: 'NG-CS-243',
    date: '2026-07-19',
    timeUtc: '1200Z',
    title: 'Lake Finland Discovery',
    dep: 'EFHV',
    arr: 'EFJO',
    route: 'EFHV - LAHTI - JYVASKYLA - EFJO',
    aircraft: PUBLIC_AIRCRAFT_TYPE,
    duration: '01H40',
    seatsTotal: 3,
    seatsAvailable: 1,
    costPerSeatEur: 130,
    status: 'LIMITED',
    highlights: ['LAKES ROUTE', 'ONE-WAY OPTION', 'LIGHT BAG ONLY'],
    notes: 'Best suited for flexible passengers; return arrangements are separate unless added by pilot.'
  }
];

const bookingRequests = [];
const bookingAvailability = new Map();
const bookingTimeline = new Map();
const bookingManageLinkThrottle = new Map();
const pilotPasskeys = new Map();
const passkeyRegistrationChallenges = new Map();
const passkeyAuthenticationChallenges = new Map();
let bookingPool = null;

async function initializeBookingStore() {
  if (!process.env.DATABASE_URL) {
    console.warn('BOOKING STORE: DATABASE_URL not set; using temporary runtime storage.');
    return;
  }

  try {
    const { Pool } = await import('pg');
    bookingPool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
    });
    await bookingPool.query(`
      CREATE TABLE IF NOT EXISTS booking_requests (
        id TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS booking_availability (
        date DATE PRIMARY KEY,
        is_open BOOLEAN NOT NULL,
        note TEXT NOT NULL DEFAULT '',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS booking_timeline (
        id TEXT PRIMARY KEY,
        request_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        note TEXT NOT NULL DEFAULT '',
        actor TEXT NOT NULL DEFAULT 'SYSTEM',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS pilot_passkeys (
        id TEXT PRIMARY KEY,
        profile_role TEXT NOT NULL,
        profile_name TEXT NOT NULL,
        credential_id TEXT NOT NULL UNIQUE,
        public_key TEXT NOT NULL,
        counter BIGINT NOT NULL DEFAULT 0,
        transports JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_used_at TIMESTAMPTZ
      );
    `);
    await bookingPool.query("ALTER TABLE booking_timeline ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT 'SYSTEM'");
    const [requests, availability, timeline, passkeys] = await Promise.all([
      bookingPool.query('SELECT payload FROM booking_requests ORDER BY created_at ASC'),
      bookingPool.query('SELECT date, is_open, note, updated_at FROM booking_availability'),
      bookingPool.query('SELECT id, request_id, event_type, note, actor, created_at FROM booking_timeline ORDER BY created_at ASC'),
      bookingPool.query('SELECT id, profile_role, profile_name, credential_id, public_key, counter, transports, created_at, last_used_at FROM pilot_passkeys ORDER BY created_at ASC')
    ]);
    bookingRequests.push(...requests.rows.map(row => row.payload));
    availability.rows.forEach(row => bookingAvailability.set(String(row.date).slice(0, 10), {
      isOpen: row.is_open,
      note: row.note,
      updatedAt: row.updated_at
    }));
    timeline.rows.forEach(row => {
      const items = bookingTimeline.get(row.request_id) || [];
      items.push({ id: row.id, type: row.event_type, note: row.note, actor: row.actor || 'SYSTEM', createdAt: row.created_at });
      bookingTimeline.set(row.request_id, items);
    });
    passkeys.rows.forEach(row => {
      pilotPasskeys.set(row.credential_id, {
        id: row.id,
        profileRole: row.profile_role,
        profileName: row.profile_name,
        credentialId: row.credential_id,
        publicKey: row.public_key,
        counter: Number(row.counter || 0),
        transports: Array.isArray(row.transports) ? row.transports : [],
        createdAt: row.created_at,
        lastUsedAt: row.last_used_at
      });
    });
    console.log(`BOOKING STORE: loaded ${bookingRequests.length} requests.`);
  } catch (err) {
    bookingPool = null;
    console.error('BOOKING STORE: database unavailable; using temporary runtime storage.', err.message);
  }
}

const bookingStoreReady = initializeBookingStore();

async function persistBookingRequest(request) {
  if (!bookingPool) return;
  await bookingPool.query(
    `INSERT INTO booking_requests (id, payload, created_at) VALUES ($1, $2::jsonb, $3)
     ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`,
    [request.id, JSON.stringify(request), request.createdAt || new Date().toISOString()]
  );
}

async function removeBookingRequest(requestId) {
  if (!bookingPool) return;
  await bookingPool.query('DELETE FROM booking_timeline WHERE request_id = $1', [requestId]);
  await bookingPool.query('DELETE FROM booking_requests WHERE id = $1', [requestId]);
}

async function persistAvailability(date, item) {
  if (!bookingPool) return;
  await bookingPool.query(
    `INSERT INTO booking_availability (date, is_open, note, updated_at) VALUES ($1, $2, $3, NOW())
     ON CONFLICT (date) DO UPDATE SET is_open = EXCLUDED.is_open, note = EXCLUDED.note, updated_at = NOW()`,
    [date, item.isOpen, item.note || '']
  );
}

async function persistPilotPasskey(passkey) {
  pilotPasskeys.set(passkey.credentialId, passkey);
  if (!bookingPool) return;
  await bookingPool.query(
    `INSERT INTO pilot_passkeys (id, profile_role, profile_name, credential_id, public_key, counter, transports, created_at, last_used_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
     ON CONFLICT (credential_id) DO UPDATE SET
       profile_role = EXCLUDED.profile_role,
       profile_name = EXCLUDED.profile_name,
       public_key = EXCLUDED.public_key,
       counter = EXCLUDED.counter,
       transports = EXCLUDED.transports,
       last_used_at = EXCLUDED.last_used_at`,
    [
      passkey.id,
      passkey.profileRole,
      passkey.profileName,
      passkey.credentialId,
      passkey.publicKey,
      passkey.counter,
      JSON.stringify(passkey.transports || []),
      passkey.createdAt || new Date().toISOString(),
      passkey.lastUsedAt || null
    ]
  );
}

async function removePilotPasskey(credentialId) {
  pilotPasskeys.delete(credentialId);
  if (!bookingPool) return;
  await bookingPool.query('DELETE FROM pilot_passkeys WHERE credential_id = $1', [credentialId]);
}

async function addBookingTimelineEvent(requestId, type, note = '', actor = 'SYSTEM') {
  const item = { id: `TML-${randomUUID().slice(0, 8).toUpperCase()}`, type, note, actor, createdAt: new Date().toISOString() };
  const items = bookingTimeline.get(requestId) || [];
  items.push(item);
  bookingTimeline.set(requestId, items);
  if (bookingPool) {
    await bookingPool.query(
      'INSERT INTO booking_timeline (id, request_id, event_type, note, actor, created_at) VALUES ($1, $2, $3, $4, $5, $6)',
      [item.id, requestId, item.type, item.note, item.actor, item.createdAt]
    );
  }
  return item;
}

async function sendBookingEmail({ to, subject, text, html = '' }) {
  const endpoint = String(process.env.GOOGLE_APPS_SCRIPT_EMAIL_URL || '').trim();
  const secret = String(process.env.GOOGLE_APPS_SCRIPT_EMAIL_SECRET || '').trim();
  if (!endpoint || !secret || !to) return false;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify({ secret, to, subject, text, html }),
      signal: controller.signal
    });
    const responseText = await response.text();
    let result = {};
    try {
      result = JSON.parse(responseText);
    } catch {
      // The diagnostic below makes a misconfigured Google Apps Script deployment visible in Render logs.
    }
    if (!response.ok || !result.ok) {
      const detail = result.error || responseText.replace(/\s+/g, ' ').slice(0, 180) || `HTTP ${response.status}`;
      throw new Error(`EMAIL RELAY FAILED ${response.status}: ${detail}`);
    }
    return true;
  } finally {
    clearTimeout(timeout);
  }
}

async function notifyPilotOfBooking(request) {
  const recipient = String(process.env.PILOT_NOTIFICATION_EMAIL || '').trim();
  return sendBookingEmail({
    to: recipient,
    subject: `New private flight request ${request.id}`,
    text: [
      `Reference: ${request.id}`,
      `Route: ${request.depName || request.dep} to ${request.arrName || request.arr}`,
      `Departure: ${request.requestDate} ${request.requestTime}`,
      `Passengers: ${request.seats}`,
      `Status: ${request.status}`,
      '',
      'Open Booking Operations to review the request.'
    ].join('\n')
  });
}

function emailPassengerNames(request) {
  const passengers = getRequestPassengers(request).filter(passenger => passenger.name);
  const names = passengers.map(passenger => {
    const title = String(passenger.title || '').trim().toUpperCase();
    const prefix = title === 'MR' ? 'Mr.' : title === 'MS' ? 'Ms.' : title === 'DR' ? 'Dr.' : '';
    return `${prefix ? `${prefix} ` : ''}${passenger.name}`.trim();
  });
  if (!names.length) return 'Private Flight Guest';
  if (names.length === 1) return names[0];
  if (names.length === 2) return `${names[0]} and ${names[1]}`;
  return `${names.slice(0, -1).join(', ')}, and ${names.at(-1)}`;
}

function emailPassengerName(passenger) {
  const title = String(passenger?.title || '').trim().toUpperCase();
  const prefix = title === 'MR' ? 'Mr.' : title === 'MS' ? 'Ms.' : title === 'DR' ? 'Dr.' : '';
  const name = String(passenger?.name || '').trim();
  return `${prefix ? `${prefix} ` : ''}${name || 'Private Flight Guest'}`.trim();
}

function passengerEmailGroups(request) {
  const groups = new Map();
  getRequestPassengers(request).forEach(passenger => {
    const email = String(passenger.email || '').trim();
    if (!email || !email.includes('@')) return;
    const key = email.toLowerCase();
    if (!groups.has(key)) groups.set(key, { email, passengers: [] });
    groups.get(key).passengers.push(passenger);
  });

  const bookingEmail = String(request.email || '').trim();
  if (!groups.size && bookingEmail.includes('@')) {
    groups.set(bookingEmail.toLowerCase(), { email: bookingEmail, passengers: [] });
  }
  return [...groups.values()];
}

function normalizeAdditionalEmailContacts(input) {
  const raw = Array.isArray(input) ? input.join(',') : String(input || '');
  const emails = raw
    .split(/[,\s;]+/)
    .map(item => item.trim())
    .filter(Boolean);
  const unique = [];
  const seen = new Set();
  for (const email of emails) {
    const lower = email.toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || seen.has(lower)) continue;
    seen.add(lower);
    unique.push(email);
  }
  return unique.slice(0, 8);
}

function bookingEmailGroups(request) {
  const groups = new Map();
  passengerEmailGroups(request).forEach(group => groups.set(group.email.toLowerCase(), group));
  normalizeAdditionalEmailContacts(request.additionalEmailContacts).forEach(email => {
    const key = email.toLowerCase();
    if (!groups.has(key)) groups.set(key, { email, passengers: [], additionalContact: true });
  });
  const payerEmail = String(request.reimbursementStatement?.payerEmail || '').trim();
  if (payerEmail && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(payerEmail)) {
    const key = payerEmail.toLowerCase();
    if (!groups.has(key)) groups.set(key, {
      email: payerEmail,
      passengers: [],
      additionalContact: true,
      payerContact: true
    });
  }
  return [...groups.values()];
}

function groupGreeting(group) {
  if (group.payerContact && !group.passengers?.length) return 'Private Flight Payer';
  if (group.additionalContact && !group.passengers?.length) return 'Private Flight Contact';
  const names = group.passengers.map(emailPassengerName);
  if (!names.length) return 'Private Flight Guest';
  if (names.length === 1) return names[0];
  if (names.length === 2) return `${names[0]} and ${names[1]}`;
  return `${names.slice(0, -1).join(', ')}, and ${names.at(-1)}`;
}

function emailDate(value) {
  const date = new Date(`${value || ''}T12:00:00`);
  return Number.isNaN(date.getTime())
    ? (value || 'To be confirmed')
    : new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'long', year: 'numeric' }).format(date);
}

function emailAirportLocation(request) {
  if (request.dep === 'EFHV') {
    return ['Hyvinkää Aerodrome', 'Lentokentäntie 50', '05820 Hyvinkää', 'Finland'];
  }
  const airport = bookingAirports.find(item => item.icao === request.dep);
  return [boardingPassGate(request.dep), airport?.name || request.depName || request.dep, airport?.country || 'Finland'];
}

function escapeEmailHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function emailFlightTime(request) {
  const time = estimateBoardingPassFlightTime(request.dep, request.arr);
  const match = String(time).match(/^(\d{2})\.(\d{2})$/);
  return match ? `${match[1]}H ${match[2]}M` : time;
}

function emailPriceSummary(request) {
  const perPassenger = request.costPerSeatEur;
  const total = request.estimatedTotalEur;
  if (typeof perPassenger === 'number' && typeof total === 'number') {
    const seats = Math.max(1, Number(request.seats || 1));
    const prefix = String(request.priceNote || '').includes('ESTIMATED') ? 'Approx. ' : '';
    return `${prefix}€${total} (${seats} PAX)`;
  }
  return String(total || perPassenger || 'Price determined by request');
}

function privateFlightEmailHtml({ status, reference, greeting, intro, details, sections = [], closing = [] }) {
  const navy = '#031c45';
  const paper = '#ffffff';
  const operationNotice = `<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-top:22px;border-collapse:collapse"><tr><td bgcolor="#f3f5f7" style="padding:12px 14px;border-left:3px solid ${navy};color:${navy};font-size:11px;line-height:1.55"><strong style="color:${navy}">PRIVATE NCO OPERATION</strong><br>This is a private, non-commercial NCO flight and remains subject to pilot decision and operational confirmation.</td></tr></table>`;
  const confidentiality = `<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-top:22px;border-collapse:collapse"><tr><td bgcolor="#fff5f5" style="padding:12px 14px;border:1px solid #b42318;color:#8f1712;font-size:11px;line-height:1.55"><strong>CONFIDENTIAL PASSENGER INFORMATION</strong><br>This email and any linked flight information may contain sensitive passenger data. It is intended only for the named recipient and must not be shared, copied, or misused.</td></tr></table>`;
  const detailRows = details.map(([label, value]) => `
    <tr><td bgcolor="${paper}" style="padding:10px 0;border-bottom:1px solid #d5d8dc;width:42%;font-weight:700;color:${navy}">${escapeEmailHtml(label)}</td><td bgcolor="${paper}" style="padding:10px 0;border-bottom:1px solid #d5d8dc;color:${navy}">${escapeEmailHtml(value)}</td></tr>`).join('');
  const sectionHtml = sections.map(section => `
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin:24px 0;border-collapse:collapse"><tr><td bgcolor="${navy}" style="padding:9px 13px;border:1px solid ${navy};color:#ffffff;font-size:13px;font-weight:700;letter-spacing:.08em">${escapeEmailHtml(section.title)}</td></tr><tr><td bgcolor="${paper}" style="padding:10px 0;color:${navy}">${section.html || (section.lines || []).map(line => `<p style="margin:8px 0;line-height:1.6;color:${navy}">${escapeEmailHtml(line)}</p>`).join('')}</td></tr></table>`).join('');
  return `<!doctype html><html><head><style>@font-face{font-family:'Computer Says No';src:url('${PUBLIC_SITE_URL}/fonts/computer-says-no.woff2') format('woff2');font-weight:700;font-style:normal}</style></head><body bgcolor="#ffffff" style="margin:0;padding:0;background-color:#ffffff;font-family:'Courier New',Courier,monospace;color:${navy}">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff;border-collapse:collapse"><tr><td align="center" bgcolor="#ffffff" style="padding:24px 12px;background-color:#ffffff">
      <table role="presentation" width="680" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;max-width:680px;background-color:#ffffff;border:2px solid ${navy};border-collapse:collapse">
        <tr><td bgcolor="#ffffff" style="padding:20px 28px;background-color:#ffffff;border-bottom:2px solid ${navy}">
          <img src="${PUBLIC_SITE_URL}/nga-private-aviation-logo.png" alt="NGA Private Aviation" width="154" style="display:block;width:154px;max-width:100%;height:auto;border:0;margin:0 0 15px" />
          <div style="font-size:12px;letter-spacing:.12em;color:${navy}">NGA PRIVATE AVIATION</div>
          <div style="margin-top:8px;font-family:'Computer Says No','Courier New',Courier,monospace;font-size:26px;font-weight:700;line-height:1;color:${navy}">${escapeEmailHtml(status)}</div>
          <div style="margin-top:13px;color:${navy};font-family:'Computer Says No','Courier New',Courier,monospace;font-size:18px;font-weight:700;line-height:1.1">REFERENCE ${escapeEmailHtml(reference)}</div>
        </td></tr>
        <tr><td bgcolor="#ffffff" style="padding:28px;background-color:#ffffff">
          <p style="margin:0 0 18px;font-size:16px;line-height:1.5;color:${navy}">Dear <strong>${escapeEmailHtml(greeting)},</strong></p>
          <p style="margin:0 0 22px;line-height:1.65;color:${navy}">${escapeEmailHtml(intro)}</p>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff;border-collapse:collapse;font-size:14px">${detailRows}</table>
          ${sectionHtml}
          ${closing.map(line => `<p style="margin:6px 0;line-height:1.5;color:${navy}">${escapeEmailHtml(line)}</p>`).join('')}
          ${operationNotice}
          ${confidentiality}
        </td></tr>
      </table>
    </td></tr></table>
  </body></html>`;
}

function requestReceivedEmailHtml({ reference, greeting, details }) {
  const navy = '#031c45';
  const lines = details.map(([label, value]) => `<tr><td bgcolor="#ffffff" style="padding:11px 0;border-bottom:1px solid #d5d8dc;color:${navy};font-size:11px;letter-spacing:.06em;text-transform:uppercase">${escapeEmailHtml(label)}</td><td align="right" bgcolor="#ffffff" style="padding:11px 0;border-bottom:1px solid #d5d8dc;color:${navy};font-size:13px;font-weight:700;line-height:1.35;text-transform:uppercase">${escapeEmailHtml(value)}</td></tr>`).join('');
  return `<!doctype html><html><head><style>@font-face{font-family:'Computer Says No';src:url('${PUBLIC_SITE_URL}/fonts/computer-says-no.woff2') format('woff2');font-weight:700;font-style:normal}</style></head><body bgcolor="#ffffff" style="margin:0;padding:0;background-color:#ffffff;font-family:'Courier New',Courier,monospace;color:${navy}">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff"><tr><td align="center" bgcolor="#ffffff" style="padding:24px 12px;background-color:#ffffff">
      <table role="presentation" width="720" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;max-width:720px;background-color:#ffffff;border:2px solid ${navy};border-collapse:collapse">
        <tr><td bgcolor="#ffffff" style="padding:20px;background-color:#ffffff">
          <img src="${PUBLIC_SITE_URL}/nga-private-aviation-logo.png" alt="NGA Private Aviation" width="154" style="display:block;width:154px;max-width:100%;height:auto;border:0;margin:0 0 15px" />
          <div style="color:${navy};font-size:11px;letter-spacing:.08em;text-transform:uppercase">Private Flight</div>
          <div style="margin:10px 0 14px;color:${navy};font-family:'Computer Says No','Courier New',Courier,monospace;font-size:26px;font-weight:700;line-height:1;text-transform:uppercase">REQUEST RECEIVED</div>
          <p style="margin:0;color:${navy};font-size:12px;line-height:1.55;text-transform:uppercase">Your flight request is with the flight team. A pilot will review the route, aircraft, weather, and loading before confirmation.</p>
          <div style="margin:16px 0;padding:10px 12px;border:1px solid ${navy};color:${navy};font-family:'Computer Says No','Courier New',Courier,monospace;font-size:19px;font-weight:700;line-height:1">REFERENCE ${escapeEmailHtml(reference)}</div>
          <p style="margin:0 0 14px;color:${navy};font-size:14px;line-height:1.5">Dear <strong>${escapeEmailHtml(greeting)},</strong></p>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff;border-collapse:collapse">${lines}</table>
          <div style="margin-top:16px;padding:12px 14px;border-left:3px solid ${navy};background-color:#f3f5f7;color:${navy};font-size:11px;line-height:1.55"><strong style="color:${navy}">PRIVATE NCO OPERATION</strong><br>This is a private, non-commercial NCO flight and remains subject to pilot decision and operational confirmation.</div>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-top:16px;width:100%;border-top:1px solid #d5d8dc;border-collapse:collapse">
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid ${navy};color:${navy};font-size:12px;text-transform:uppercase"><strong>Request received</strong><br><span style="color:${navy}">Your flight details have been received.</span></td></tr>
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid #c5c9ce;color:${navy};font-size:12px;text-transform:uppercase"><strong>Pilot and operations review</strong><br><span style="color:${navy}">Route, weather, aircraft, and loading are checked.</span></td></tr>
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid #c5c9ce;color:${navy};font-size:12px;text-transform:uppercase"><strong>Payment instructions after approval</strong><br><span style="color:${navy}">A Reimbursement Statement is issued once confirmed.</span></td></tr>
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid #c5c9ce;color:${navy};font-size:12px;text-transform:uppercase"><strong>Final flight confirmation</strong><br><span style="color:${navy}">Passenger passes and final details are sent by email.</span></td></tr>
          </table>
          <div style="margin-top:16px;padding:11px 13px;border:1px solid ${navy};color:${navy};font-size:12px;line-height:1.55"><strong style="color:${navy}">PAYMENT TERMS</strong><br>Once the flight is confirmed, a Reimbursement Statement will be issued. Payment is due no later than 48 hours before scheduled departure unless operations agrees otherwise. If payment is not received by then, the booking may be cancelled.</div>
          <p style="margin:18px 0 0;color:${navy};font-size:12px;line-height:1.55">Best regards,<br><strong>NGA Private Aviation Team</strong><br><a href="mailto:info.ngaprivateaviation@gmail.com" style="color:#007aff;text-decoration:underline">info.ngaprivateaviation@gmail.com</a></p>
          <div style="margin-top:16px;padding:12px 14px;border:1px solid #b42318;background-color:#fff5f5;color:#8f1712;font-size:11px;line-height:1.55"><strong>CONFIDENTIAL PASSENGER INFORMATION</strong><br>This email and any linked flight information may contain sensitive passenger data. It is intended only for the named recipient and must not be shared, copied, or misused.</div>
        </td></tr>
      </table>
    </td></tr></table>
  </body></html>`;
}

function passengerPassLinks(request) {
  return getRequestPassengers(request)
    .filter(passenger => passenger.boardingPassToken)
    .map(passenger => `${passenger.name || `Passenger ${passenger.number || 1}`}: ${PUBLIC_SITE_URL}/private-flight-information-pass/${encodeURIComponent(request.id)}/${passenger.boardingPassToken}`);
}

function passengerPassItems(request) {
  return getRequestPassengers(request)
    .filter(passenger => passenger.boardingPassToken)
    .map(passenger => ({
      number: passenger.number,
      label: passenger.name || `Passenger ${passenger.number || 1}`,
      email: passenger.email || '',
      url: `${PUBLIC_SITE_URL}/private-flight-information-pass/${encodeURIComponent(request.id)}/${passenger.boardingPassToken}`
    }));
}

function privateFlightAgreementItems(request) {
  return getRequestPassengers(request)
    .filter(passenger => passenger.agreementToken)
    .map(passenger => ({
      number: passenger.number,
      label: passenger.name || `Passenger ${passenger.number || 1}`,
      email: passenger.email || '',
      url: `${PUBLIC_SITE_URL}/private-flight-agreement/${encodeURIComponent(request.id)}/${passenger.agreementToken}`,
      signedAt: passenger.agreementSignedAt || ''
    }));
}

function reimbursementStatementPublicUrl(request) {
  if (!request?.reimbursementStatement || !request?.reimbursementStatementToken) return '';
  return `${PUBLIC_SITE_URL}/private-flight-reimbursement-statement/${encodeURIComponent(request.id)}/${request.reimbursementStatementToken}`;
}

function bookingPaymentUrl(request) {
  const template = String(process.env.BOOKING_PAYMENT_URL || '').trim();
  if (!template.startsWith('https://')) return '';
  const paymentReference = request?.reimbursementStatement?.paymentReference || request?.id || '';
  return template
    .replaceAll('{reference}', encodeURIComponent(request?.id || ''))
    .replaceAll('{payment_reference}', encodeURIComponent(paymentReference));
}

function proposedTimeLabel(proposal) {
  if (!proposal?.date || !proposal?.time) return 'To be confirmed';
  return `${emailDate(proposal.date)} at ${formatBoardingPassTime(proposal.time)} Local Time`;
}

function timeProposalResponseUrl(request, action) {
  if (!request?.timeProposal?.token) return '';
  return `${PUBLIC_SITE_URL}/private-flight-time-proposal/${encodeURIComponent(request.id)}/${request.timeProposal.token}/${action}`;
}

async function notifyBookerOfTimeProposal(request) {
  const proposal = request.timeProposal;
  if (!proposal?.token) return 0;
  const acceptUrl = timeProposalResponseUrl(request, 'accept');
  const denyUrl = timeProposalResponseUrl(request, 'deny');
  const currentTime = `${emailDate(request.requestDate)} at ${formatBoardingPassTime(request.requestTime)} Local Time`;
  const proposedTime = proposedTimeLabel(proposal);
  const details = [
    ['Route', `${boardingPassAirportLabel(request.dep, request.depName)} - ${boardingPassAirportLabel(request.arr, request.arrName)}`],
    ['Current requested departure', currentTime],
    ['Proposed new departure', proposedTime],
    ['Reason', proposal.note || 'Operational timing adjustment'],
    ['Reference', request.id]
  ];
  const responseHtml = `
    <p style="margin:10px 0 14px;line-height:1.6;color:#031c45">Please review the proposed departure time below. You can accept it or decline it and add a short message for operations.</p>
    <p style="margin:16px 0">
      <a href="${escapeEmailHtml(acceptUrl)}" style="display:inline-block;background:#0f7b42;color:#ffffff;text-decoration:none;font-weight:700;padding:12px 18px;border-radius:3px;margin:0 8px 8px 0">ACCEPT NEW TIME</a>
      <a href="${escapeEmailHtml(denyUrl)}" style="display:inline-block;background:#b42318;color:#ffffff;text-decoration:none;font-weight:700;padding:12px 18px;border-radius:3px;margin:0 0 8px">DECLINE NEW TIME</a>
    </p>
    <p style="margin:10px 0;line-height:1.55;color:#031c45">After opening either link, you may add a message for the flight team before submitting your response.</p>`;

  const groups = bookingEmailGroups(request);
  await Promise.all(groups.map(group => sendBookingEmail({
    to: group.email,
    subject: `Departure time proposal / ${request.id}`,
    text: [
      `Dear ${groupGreeting(group)},`,
      '',
      'Operations would like to propose a new departure time for your private flight request.',
      '',
      `Reference: ${request.id}`,
      `Route: ${details[0][1]}`,
      `Current requested departure: ${currentTime}`,
      `Proposed new departure: ${proposedTime}`,
      `Reason: ${proposal.note || 'Operational timing adjustment'}`,
      '',
      `Accept new time: ${acceptUrl}`,
      `Decline new time: ${denyUrl}`,
      '',
      'You can add a short message for operations after opening either link.',
      '',
      'Best regards,',
      'NGA Private Aviation Team',
      'info.ngaprivateaviation@gmail.com'
    ].join('\n'),
    html: privateFlightEmailHtml({
      status: 'NEW TIME PROPOSED',
      reference: request.id,
      greeting: groupGreeting(group),
      intro: 'Operations would like to propose a new departure time for your private flight request.',
      details,
      sections: [{ title: 'PASSENGER RESPONSE REQUIRED', html: responseHtml }],
      closing: ['Best regards,', 'NGA Private Aviation Team', 'info.ngaprivateaviation@gmail.com']
    })
  })));
  return groups.length;
}

async function notifyBookerOfBooking(request) {
  const details = [
    ['Route', `${boardingPassAirportLabel(request.dep, request.depName)} - ${boardingPassAirportLabel(request.arr, request.arrName)}`],
    ['Date', emailDate(request.requestDate)],
    ['Requested departure', `${formatBoardingPassTime(request.requestTime)} Local Time`],
    ['Estimated flight time', emailFlightTime(request)],
    ['Passengers', request.seats],
    ['Baggage', boardingPassBaggage(request)],
    ['Price estimate', emailPriceSummary(request)]
  ];
  const groups = bookingEmailGroups(request);
  await Promise.all(groups.map(group => sendBookingEmail({
    to: group.email,
    subject: `Private flight request received / ${request.id}`,
    text: [
      `Dear ${groupGreeting(group)},`,
      '',
      'Thank you for your private flight request. Our operations team has received it and will contact you as soon as possible after pilot and operational review.',
      '',
      'REQUESTED FLIGHT DETAILS',
      `Booking reference: ${request.id}`,
      `Route: ${details[0][1]}`,
      `Date: ${emailDate(request.requestDate)}`,
      `Requested departure: ${formatBoardingPassTime(request.requestTime)} Local Time`,
      `Estimated flight time: ${emailFlightTime(request)}`,
      `Passengers: ${request.seats}`,
      `Baggage: ${boardingPassBaggage(request)}`,
      `Price estimate: ${emailPriceSummary(request)}`,
      '',
      'This is a request only and is not a flight confirmation. A pilot will review the route, weather, aircraft availability, loading, and operational requirements before confirming the flight.',
      'Once the flight is confirmed, a Reimbursement Statement will be issued. Payment is due no later than 48 hours before scheduled departure unless operations agrees otherwise. If payment is not received by then, the booking may be cancelled.',
      '',
      'Best regards,',
      'NGA Private Aviation Team',
      'info.ngaprivateaviation@gmail.com'
    ].join('\n'),
    html: requestReceivedEmailHtml({
      reference: request.id,
      greeting: groupGreeting(group),
      details
    })
  })));
  return groups.length;
}

async function notifyBookerOfApproval(request) {
  const groups = bookingEmailGroups(request);
  const allPassItems = passengerPassItems(request);
  const allAgreementItems = privateFlightAgreementItems(request);
  const reimbursementUrl = reimbursementStatementPublicUrl(request);
  const requestedPaymentContact = String(request.reimbursementStatement?.payerEmail || request.email || '').trim().toLowerCase();
  const paymentContactEmail = groups.some(group => group.email.toLowerCase() === requestedPaymentContact)
    ? requestedPaymentContact
    : String(groups[0]?.email || '').trim().toLowerCase();
  const details = [
    ['Route', `${boardingPassAirportLabel(request.dep, request.depName)} - ${boardingPassAirportLabel(request.arr, request.arrName)}`],
    ['Date', emailDate(request.requestDate)],
    ['Boarding time', `${boardingPassBoardingTime(request.requestTime)} Local Time`],
    ['Scheduled departure', `${formatBoardingPassTime(request.requestTime)} Local Time`],
    ['Aircraft', PUBLIC_AIRCRAFT_TYPE],
    ['Total price', emailPriceSummary(request)]
  ];
  await Promise.all(groups.map(group => {
    const passengerNumbers = new Set(group.passengers.map(passenger => passenger.number));
    const passItems = allPassItems.filter(item => passengerNumbers.has(item.number));
    const agreementItems = allAgreementItems.filter(item => passengerNumbers.has(item.number));
    const passLinks = passItems.map(item => `${item.label}: ${item.url}`);
    const isPaymentContact = group.email.toLowerCase() === paymentContactEmail;
    const passHtml = passItems.length
      ? passItems.map(item => `<p style="margin:10px 0 6px;color:#031c45"><strong>${escapeEmailHtml(item.label)}</strong><br><a href="${escapeEmailHtml(item.url)}" style="display:inline-block;margin-top:5px;color:#b42318;font-weight:700;text-decoration:underline;letter-spacing:.02em">FLIGHT INFORMATION PASS / OPEN HERE</a></p>`).join('')
      : '<p style="margin:8px 0;line-height:1.55;color:#031c45">Passenger passes will be issued by operations shortly.</p>';
    const documentHtml = agreementItems.length
      ? agreementItems.map(item => `<p style="margin:10px 0;color:#031c45"><strong>${escapeEmailHtml(item.label)}</strong><br><a href="${escapeEmailHtml(item.url)}" style="display:inline-block;margin-top:5px;color:#007aff;font-weight:700;text-decoration:underline">OPEN PRIVATE FLIGHT AGREEMENT</a> <span style="color:#b42318;font-weight:700">(E-SIGNATURE REQUIRED)</span></p>`).join('')
      : '<p style="margin:8px 0;color:#031c45">Private Flight Agreement: to be provided by operations.</p>';
    const paymentTermsHtml = isPaymentContact
      ? [
        '<p style="margin:8px 0;line-height:1.6;color:#031c45">Payment is due no later than 48 hours before scheduled departure unless operations agrees otherwise. If payment is not received by then, the booking may be cancelled.</p>',
        reimbursementUrl
          ? `<p style="margin:12px 0 0"><a href="${escapeEmailHtml(reimbursementUrl)}" style="color:#007aff;font-weight:700;text-decoration:underline">OPEN REIMBURSEMENT STATEMENT</a> <span style="color:#b42318;font-weight:700">(PAYMENT REQUIRED)</span></p>`
          : '<p style="margin:12px 0 0;color:#031c45">Reimbursement Statement: to be provided by operations.</p>'
      ].join('')
      : '<p style="margin:8px 0;line-height:1.6;color:#031c45">Payment arrangements for this booking are coordinated with the lead booking contact. Please complete your own Private Flight Agreement before the flight.</p>';
    return sendBookingEmail({
      to: group.email,
      subject: `Flight confirmed / ${request.id}`,
      text: [
        `Dear ${groupGreeting(group)},`,
        '',
        'We are pleased to confirm that your private flight request has been approved, subject to normal day-of-flight weather and operational checks.',
        '',
        'DEPARTURE DETAILS',
        `Route: ${details[0][1]}`,
        `Date: ${emailDate(request.requestDate)}`,
        `Boarding time: ${boardingPassBoardingTime(request.requestTime)} Local Time`,
        `Scheduled departure: ${formatBoardingPassTime(request.requestTime)} Local Time`,
        `Aircraft: ${details[4][1]}`,
        `Total price: ${emailPriceSummary(request)}`,
        '',
        'AIRPORT LOCATION',
        ...emailAirportLocation(request),
        '',
        'Please arrive no later than the boarding time and bring a valid form of identification. Keep your mobile phone available for any operational update, and have baggage ready for loading on arrival.',
        '',
        'YOUR FLIGHT INFORMATION PASS',
        ...(passLinks.length ? passLinks : ['Passenger passes will be issued by operations shortly.']),
        '',
        'YOUR PASSENGER DOCUMENTATION',
        ...(agreementItems.length ? agreementItems.map(item => `Private Flight Agreement / ${item.label} (E-SIGNATURE REQUIRED): ${item.url}`) : ['Private Flight Agreement: to be provided by operations.']),
        '',
        'PAYMENT TERMS',
        ...(isPaymentContact
          ? [
            'A Reimbursement Statement will be issued for this confirmed flight. Payment is due no later than 48 hours before scheduled departure unless operations agrees otherwise. If payment is not received by then, the booking may be cancelled.',
            reimbursementUrl ? `Open Reimbursement Statement (PAYMENT REQUIRED): ${reimbursementUrl}` : 'Reimbursement Statement: to be provided by operations.'
          ]
          : ['Payment arrangements for this booking are coordinated with the lead booking contact. Please complete your own Private Flight Agreement before the flight.']),
        '',
        'BAGGAGE AND ITEMS ON BOARD',
        'Personal bags, electronics, medication, and normal personal items may be carried subject to pilot approval. Do not bring weapons, explosives, flammable liquids or gases, dangerous goods, or undeclared lithium batteries.',
        '',
        'Please notify us as soon as possible if your plans or baggage change, or if you expect to arrive late.',
        '',
        'Best regards,',
        'NGA Private Aviation Team',
        'info.ngaprivateaviation@gmail.com'
      ].join('\n'),
      html: privateFlightEmailHtml({
        status: 'FLIGHT CONFIRMED',
        reference: request.id,
        greeting: groupGreeting(group),
        intro: 'We are pleased to confirm that your private flight request has been approved, subject to normal day-of-flight weather and operational checks.',
        details,
        sections: [
          { title: 'AIRPORT LOCATION', lines: emailAirportLocation(request) },
          { title: 'YOUR FLIGHT INFORMATION PASS', html: passHtml },
          { title: 'YOUR PASSENGER DOCUMENTATION', html: documentHtml },
          { title: 'PAYMENT TERMS', html: paymentTermsHtml },
          { title: 'BAGGAGE AND ITEMS ON BOARD', lines: ['Personal bags, electronics, medication, and normal personal items may be carried subject to pilot approval. Do not bring weapons, explosives, flammable liquids or gases, dangerous goods, or undeclared lithium batteries.'] },
          { title: 'BOARDING REMINDER', lines: ['Please arrive no later than the boarding time and bring a valid form of identification. Keep your mobile phone available for any operational update, and have baggage ready for loading on arrival.'] }
        ],
        closing: ['Best regards,', 'NGA Private Aviation Team', 'info.ngaprivateaviation@gmail.com']
      })
    });
  }));
  return groups.length;
}

async function deliverBookingNotifications(request) {
  try {
    if (await notifyPilotOfBooking(request)) {
      await addBookingTimelineEvent(request.id, 'PILOT EMAIL NOTIFICATION', 'New booking notification sent.');
    }
  } catch (err) {
    console.error('BOOKING EMAIL:', err.message);
  }
  try {
    const recipientCount = await notifyBookerOfBooking(request);
    if (recipientCount) {
      await addBookingTimelineEvent(request.id, 'PASSENGER EMAIL NOTIFICATION', `Booking receipt sent to ${recipientCount} passenger email${recipientCount === 1 ? '' : 's'}.`);
    }
  } catch (err) {
    console.error('BOOKER EMAIL:', err.message);
  }
}

async function deliverApprovalNotification(request) {
  try {
    const recipientCount = await notifyBookerOfApproval(request);
    if (recipientCount) {
      request.confirmationEmailSentAt = new Date().toISOString();
      await persistBookingRequest(request);
      await addBookingTimelineEvent(request.id, 'FLIGHT CONFIRMATION EMAIL', `Passenger flight confirmation and individual agreement links sent to ${recipientCount} passenger email${recipientCount === 1 ? '' : 's'}.`);
    }
  } catch (err) {
    console.error('CONFIRMATION EMAIL:', err.message);
  }
}

const bookingAirports = [
  { icao: 'EFHK', short: 'HEL', name: 'Helsinki-Vantaa', city: 'Helsinki', country: 'Finland', type: 'controlled', lat: 60.3172, lon: 24.9633 },
  { icao: 'EFHV', short: 'HYV', name: 'Hyvinkaa', city: 'Hyvinkaa', country: 'Finland', type: 'GA / uncontrolled', lat: 60.6544, lon: 24.8811 },
  { icao: 'EFNU', short: 'NUM', name: 'Nummela', city: 'Nummela', country: 'Finland', type: 'GA / uncontrolled', lat: 60.3339, lon: 24.2964 },
  { icao: 'EFPR', short: 'PYT', name: 'Pyhtaa Redstone', city: 'Pyhtaa', country: 'Finland', type: 'GA / uncontrolled', lat: 60.4844, lon: 26.5439 },
  { icao: 'EFHN', short: 'HNK', name: 'Hanko', city: 'Hanko', country: 'Finland', type: 'GA / uncontrolled', lat: 59.8489, lon: 23.0836 },
  { icao: 'EFRY', short: 'RAY', name: 'Rayskala', city: 'Rayskala', country: 'Finland', type: 'GA / uncontrolled', lat: 60.7447, lon: 24.1078 },
  { icao: 'EFLA', short: 'LAH', name: 'Lahti-Vesivehmaa', city: 'Lahti', country: 'Finland', type: 'GA / uncontrolled', lat: 61.1442, lon: 25.6935 },
  { icao: 'EFTU', short: 'TKU', name: 'Turku', city: 'Turku', country: 'Finland', type: 'controlled', lat: 60.5141, lon: 22.2628 },
  { icao: 'EFTP', short: 'TMP', name: 'Tampere-Pirkkala', city: 'Tampere', country: 'Finland', type: 'controlled', lat: 61.4141, lon: 23.6044 },
  { icao: 'EETN', short: 'TLL', name: 'Tallinn', city: 'Tallinn', country: 'Estonia', type: 'controlled', lat: 59.4133, lon: 24.8328 },
  { icao: 'EEKE', short: 'URE', name: 'Kuressaare', city: 'Kuressaare', country: 'Estonia', type: 'regional / GA', lat: 58.2300, lon: 22.5095 },
  { icao: 'ESSB', short: 'BMA', name: 'Stockholm Bromma', city: 'Stockholm', country: 'Sweden', type: 'controlled / GA', lat: 59.3544, lon: 17.9417 }
];

function nextBookingReference(depIcao, arrIcao) {
  const dep = bookingAirports.find(airport => airport.icao === depIcao);
  const arr = bookingAirports.find(airport => airport.icao === arrIcao);
  const route = `${dep?.short || depIcao || 'DEP'}-${arr?.short || arrIcao || 'ARR'}`.toUpperCase();
  const prefix = `${route}-`;
  const highest = bookingRequests.reduce((max, request) => {
    if (!String(request.id || '').startsWith(prefix)) return max;
    const sequence = Number(String(request.id).slice(prefix.length));
    return Number.isInteger(sequence) ? Math.max(max, sequence) : max;
  }, 0);
  return `${prefix}${String(highest + 1).padStart(3, '0')}`;
}

const BOOKING_TERMINALS = {
  EFHK: 'FINAVIA FBO',
  EETN: 'FBO TALLINN VIP',
  ESSB: 'IFLY FBO'
};

const EUROPE_COUNTRIES = new Set([
  'AL', 'AD', 'AT', 'BE', 'BA', 'BG', 'BY', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE',
  'ES', 'FI', 'FO', 'FR', 'GB', 'GG', 'GI', 'GR', 'HR', 'HU', 'IE', 'IM', 'IS',
  'IT', 'JE', 'LI', 'LT', 'LU', 'LV', 'MC', 'MD', 'ME', 'MK', 'MT', 'NL', 'NO',
  'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'SM', 'UA', 'VA'
]);

const RUNWAY_IDENT_RE = /^(0?[1-9]|[1-2][0-9]|3[0-6])([LCRT])?$/i;

const AIRCRAFT_WEIGHT_BALANCE_PROFILE = Object.freeze({
  profileVersion: 'FOREFLIGHT-2026-07-16',
  locked: true,
  weightUnits: 'LB',
  armUnits: 'IN',
  basicEmptyWeightLbs: 1727,
  basicEmptyCgIn: 86.32,
  gearRetractionMomentChangeInLb: 819,
  maxZeroFuelWeightLbs: 2650,
  maxRampWeightLbs: 2657,
  maxTakeoffWeightLbs: 2650,
  maxLandingWeightLbs: 2650,
  fuelWeightPerGallonLbs: 6,
  stations: Object.freeze({
    cockpit: Object.freeze({ armIn: 80.5, seats: 2, weightLimitLbs: null }),
    rearSeats: Object.freeze({ armIn: 118.1, seats: 2, weightLimitLbs: null }),
    baggage: Object.freeze({ armIn: 142.8, weightLimitLbs: 200 }),
    fuel: Object.freeze({ armIn: 95, weightLimitLbs: 288, usableGallons: 48 })
  }),
  forwardCgLimits: Object.freeze([
    Object.freeze({ weightLbs: 1400, cgIn: 80 }),
    Object.freeze({ weightLbs: 1800, cgIn: 80 }),
    Object.freeze({ weightLbs: 2300, cgIn: 82 }),
    Object.freeze({ weightLbs: 2650, cgIn: 87.3 })
  ]),
  aftCgLimits: Object.freeze([
    Object.freeze({ weightLbs: 1400, cgIn: 93 }),
    Object.freeze({ weightLbs: 2650, cgIn: 93 })
  ])
});

const AIRCRAFT_PROFILE = {
  registration: 'OH-PMK',
  type: 'Piper PA-28R-200 Arrow II',
  bestGlideSpeedKt: 91,
  bestGlideRatio: 9,
  defaultCruiseAltitudeFt: 8000,
  maxCeilingFt: 17000,
  basicEmptyWeightLbs: AIRCRAFT_WEIGHT_BALANCE_PROFILE.basicEmptyWeightLbs,
  maxTakeoffWeightLbs: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxTakeoffWeightLbs,
  maxLandingWeightLbs: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxLandingWeightLbs,
  maxRampWeightLbs: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxRampWeightLbs,
  fuelType: '100LL',
  startTaxiTakeoffFuelGal: 1.17,
  climbTasKt: 95,
  climbFuelFlowGph: 10,
  climbRateFpm: 600,
  cruiseTasKt: 115,
  cruiseFuelFlowGph: 8,
  descentTasKt: 115,
  descentFuelFlowGph: 8,
  descentRateFpm: 500,
  takeoff: {
    powerSetting: 'FULL',
    ac: 'OFF',
    defaultFlaps: '0°',
    rotatePitch: '7–9° NOSE UP',
    xwindLimitKt: 17,
    limitCode: 'MTOW'
  },
  landing: {
    ac: 'OFF',
    defaultFlaps: 'FULL (40°)',
    braking: 'MAX/MOD/CRM',
    goAroundPower: 'FULL',
    limitCode: 'MLDW'
  }
};

let airportDbCache = {
  data: null,
  loadedAt: 0,
  promise: null
};

let runwayDbCache = {
  data: null,
  loadedAt: 0,
  promise: null
};

const responseCache = new Map();

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

function normalizeIcao(input) {
  return String(input || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, '')
    .slice(0, 4);
}

function normalizeFlight(input) {
  return String(input || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, 10);
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
  return String(input || '')
    .trim()
    .replace(/\s+/g, ' ')
    .toUpperCase()
    .slice(0, 80);
}

function normalizeHeading(input) {
  const digits = String(input || '').replace(/[^0-9]/g, '').slice(0, 3);
  if (!digits) return null;

  const value = parseInt(digits, 10);
  if (!Number.isFinite(value)) return null;
  if (value < 0 || value > 360) return null;

  return value === 360 ? 0 : value;
}

function normalizeWeight(input) {
  const num = Number(input);
  return Number.isFinite(num) ? Math.round(num) : null;
}

function normalizeDecimal(input, digits = 2) {
  const num = Number(input);
  if (!Number.isFinite(num)) return null;
  const factor = 10 ** digits;
  return Math.round(num * factor) / factor;
}

function parseBoolean(v) {
  return String(v).toLowerCase() === 'true';
}

function parseCookies(req) {
  return String(req.headers.cookie || '')
    .split(';')
    .map(part => part.trim())
    .filter(Boolean)
    .reduce((cookies, part) => {
      const eq = part.indexOf('=');
      if (eq === -1) return cookies;
      cookies[decodeURIComponent(part.slice(0, eq))] = decodeURIComponent(part.slice(eq + 1));
      return cookies;
    }, {});
}

function pilotAccessProfiles() {
  const profiles = [
    {
      code: PILOT_ACCESS_CODE,
      role: 'COMMANDER',
      name: normalizeBookingText(process.env.PILOT_COMMANDER_NAME || 'PILOT COMMANDER', 60).toUpperCase()
    }
  ];
  const optionalProfiles = [
    ['PILOT_SECONDARY_CODE', 'PILOT_SECONDARY_NAME', 'SECONDARY', 'SECONDARY PILOT'],
    ['PILOT_DISPATCH_CODE', 'PILOT_DISPATCH_NAME', 'DISPATCH', 'FLIGHT DISPATCH']
  ];
  optionalProfiles.forEach(([codeKey, nameKey, role, fallbackName]) => {
    const code = String(process.env[codeKey] || '').trim();
    if (!code || profiles.some(profile => profile.code === code)) return;
    profiles.push({
      code,
      role,
      name: normalizeBookingText(process.env[nameKey] || fallbackName, 60).toUpperCase()
    });
  });
  return profiles;
}

function webAuthnConfiguration() {
  let publicUrl;
  try {
    publicUrl = new URL(PUBLIC_SITE_URL);
  } catch {
    publicUrl = new URL('https://ngaprivateaviation.com');
  }
  const rpID = String(process.env.WEBAUTHN_RP_ID || publicUrl.hostname).trim().toLowerCase();
  const expectedOrigin = String(process.env.WEBAUTHN_ORIGIN || publicUrl.origin).trim().replace(/\/$/, '');
  return { rpID, expectedOrigin };
}

function clearExpiredPasskeyChallenges() {
  const expiresBefore = Date.now() - WEBAUTHN_CHALLENGE_TTL_MS;
  [passkeyRegistrationChallenges, passkeyAuthenticationChallenges].forEach(store => {
    for (const [id, value] of store.entries()) {
      if (Number(value.createdAt || 0) < expiresBefore) store.delete(id);
    }
  });
}

function rememberPasskeyChallenge(store, value) {
  clearExpiredPasskeyChallenges();
  const id = `PK-${randomUUID()}`;
  store.set(id, { ...value, createdAt: Date.now() });
  return id;
}

function consumePasskeyChallenge(store, id) {
  clearExpiredPasskeyChallenges();
  const value = store.get(String(id || ''));
  if (!value) return null;
  store.delete(String(id || ''));
  return value;
}

function passkeyProfileKey(profile) {
  return String(profile?.role || '').trim().toUpperCase();
}

function passkeysForProfile(profile) {
  const profileKey = passkeyProfileKey(profile);
  return [...pilotPasskeys.values()].filter(item => item.profileRole === profileKey);
}

function passkeyPublicView(passkey) {
  return {
    id: passkey.id,
    credentialId: passkey.credentialId,
    createdAt: passkey.createdAt,
    lastUsedAt: passkey.lastUsedAt || null,
    transports: Array.isArray(passkey.transports) ? passkey.transports : []
  };
}

function pilotSessionSignature(payload) {
  return createHmac('sha256', PILOT_SESSION_SECRET).update(payload).digest('base64url');
}

function makePilotSession(profile) {
  const payload = Buffer.from(JSON.stringify({
    role: profile.role,
    name: profile.name,
    exp: Math.floor(Date.now() / 1000) + PILOT_SESSION_MAX_AGE_SECONDS
  })).toString('base64url');
  return `${payload}.${pilotSessionSignature(payload)}`;
}

function readPilotSession(req) {
  const token = parseCookies(req)[PILOT_COOKIE_NAME];
  if (!token || !token.includes('.')) return null;
  const [payload, signature] = token.split('.');
  if (!payload || !signature) return null;
  const expected = pilotSessionSignature(payload);
  const receivedBytes = Buffer.from(signature);
  const expectedBytes = Buffer.from(expected);
  if (receivedBytes.length !== expectedBytes.length || !timingSafeEqual(receivedBytes, expectedBytes)) return null;
  try {
    const session = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    if (!session?.role || !session?.name || Number(session.exp) <= Math.floor(Date.now() / 1000)) return null;
    return { role: String(session.role), name: String(session.name), exp: Number(session.exp) };
  } catch {
    return null;
  }
}

function hasPilotAccess(req) {
  return Boolean(readPilotSession(req));
}

function pilotActor(req) {
  const session = req.pilotSession || readPilotSession(req);
  return session ? `${session.role} / ${session.name}` : 'SYSTEM';
}

function requirePilotAccess(req, res, next) {
  const session = readPilotSession(req);
  if (session) {
    req.pilotSession = session;
    return next();
  }
  return res.status(401).json({ error: 'PILOT ACCESS REQUIRED' });
}

function requireFlightCrew(req, res) {
  if (['COMMANDER', 'SECONDARY'].includes(req.pilotSession?.role)) return true;
  res.status(403).json({ error: 'COMMANDER OR SECONDARY PILOT ACCESS REQUIRED' });
  return false;
}

function pilotCookieOptions(profile) {
  const secure = process.env.NODE_ENV === 'production';
  return [
    `${PILOT_COOKIE_NAME}=${makePilotSession(profile)}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
    `Max-Age=${PILOT_SESSION_MAX_AGE_SECONDS}`,
    secure ? 'Secure' : ''
  ].filter(Boolean).join('; ');
}

function normalizeBookingText(input, max = 80) {
  return String(input || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, max);
}

function normalizeReimbursementStatement(input) {
  if (!input || typeof input !== 'object') return null;
  const charges = Array.isArray(input.charges) ? input.charges.slice(0, 8).map(charge => ({
    description: normalizeBookingText(charge?.description, 80),
    quantity: normalizeBookingText(charge?.quantity, 30),
    unitPrice: normalizeBookingText(charge?.unitPrice, 20),
    amount: normalizeBookingText(charge?.amount, 20)
  })) : [];
  return {
    statementDate: normalizeBookingText(input.statementDate, 30),
    dueDate: normalizeBookingText(input.dueDate, 30),
    deliveryDate: normalizeBookingText(input.deliveryDate, 30),
    crewName: normalizeBookingText(input.crewName, 60),
    crewEmail: normalizeBookingText(input.crewEmail, 120),
    crewPhone: normalizeBookingText(input.crewPhone, 40),
    passengerNumber: normalizeBookingText(input.passengerNumber, 8),
    passengerName: normalizeBookingText(input.passengerName, 60),
    passengerEmail: normalizeBookingText(input.passengerEmail, 120),
    passengerPhone: normalizeBookingText(input.passengerPhone, 40),
    payerName: normalizeBookingText(input.payerName, 80),
    payerEmail: normalizeBookingText(input.payerEmail, 120),
    payerPhone: normalizeBookingText(input.payerPhone, 40),
    bankName: normalizeBookingText(input.bankName, 60),
    accountName: normalizeBookingText(input.accountName, 80),
    iban: normalizeBookingText(input.iban, 60),
    bic: normalizeBookingText(input.bic, 30),
    siirtoPhone: normalizeBookingText(input.siirtoPhone, 40),
    paymentReference: normalizeBookingText(input.paymentReference, 80),
    charges
  };
}

function operationalPlanNumber(input, fallback = 0, min = -100000, max = 100000) {
  const value = Number(String(input ?? '').replace(',', '.'));
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(max, Math.round(value * 100) / 100));
}

function normalizeOperationalPerformanceSnapshot(input) {
  if (!input || typeof input !== 'object') return null;
  const numberOrNull = (value, min = -100000, max = 100000) => {
    if (value === null || value === undefined || String(value).trim() === '') return null;
    const number = Number(value);
    if (!Number.isFinite(number)) return null;
    return Math.max(min, Math.min(max, Math.round(number * 100) / 100));
  };
  const normalizeLeg = leg => ({
    airport: normalizeBookingText(leg?.airport, 8).toUpperCase(),
    runway: normalizeBookingText(leg?.runway, 12).toUpperCase(),
    headingDeg: numberOrNull(leg?.headingDeg, 0, 360),
    runwayLengthFt: numberOrNull(leg?.runwayLengthFt, 0, 30000),
    surface: normalizeBookingText(leg?.surface, 40).toUpperCase(),
    widthFt: numberOrNull(leg?.widthFt, 0, 1000),
    windRaw: normalizeBookingText(leg?.windRaw, 120).toUpperCase(),
    headwindKt: numberOrNull(leg?.headwindKt, -200, 200),
    crosswindKt: numberOrNull(leg?.crosswindKt, -200, 200),
    oatC: numberOrNull(leg?.oatC, -100, 100),
    qnhHpa: numberOrNull(leg?.qnhHpa, 800, 1200),
    densityAltitudeFt: numberOrNull(leg?.densityAltitudeFt, -5000, 30000),
    elevationFt: numberOrNull(leg?.elevationFt, -2000, 30000),
    plannedWeightLb: numberOrNull(leg?.plannedWeightLb, 0, 10000)
  });
  return {
    capturedAt: normalizeBookingText(input.capturedAt, 40),
    source: normalizeBookingText(input.source, 80).toUpperCase() || 'NGA WEATHER / AIRPORT DATABASE',
    departure: normalizeLeg(input.departure),
    arrival: normalizeLeg(input.arrival)
  };
}

function normalizeOperationalFlightPlan(input, request) {
  if (!input || typeof input !== 'object') return null;
  const defaults = operationalFlightPlanDefaults(request);
  const textField = (name, max = 80) => normalizeBookingText(input[name] ?? defaults[name], max);
  const numberField = (name, min = 0, max = 10000) => operationalPlanNumber(input[name], defaults[name], min, max);
  const optionalNumberField = (name, min = 0, max = 10000) => {
    const raw = input[name];
    if (raw === null || raw === undefined || String(raw).trim() === '') return null;
    return operationalPlanNumber(raw, null, min, max);
  };
  const plan = {
    version: 'NGA-OFP-2026-07',
    updatedAt: new Date().toISOString(),
    dateUtc: textField('dateUtc', 20),
    aircraftRegistration: textField('aircraftRegistration', 16).toUpperCase(),
    aircraftModel: textField('aircraftModel', 40).toUpperCase(),
    aircraftCode: textField('aircraftCode', 24).toUpperCase(),
    commanderName: textField('commanderName', 60).toUpperCase(),
    commanderPhone: textField('commanderPhone', 40),
    callsign: textField('callsign', 40).toUpperCase(),
    departure: normalizeBookingText(request?.dep || defaults.departure, 8).toUpperCase(),
    destination: normalizeBookingText(request?.arr || defaults.destination, 8).toUpperCase(),
    depRunway: textField('depRunway', 12).toUpperCase(),
    arrRunway: textField('arrRunway', 12).toUpperCase(),
    route: textField('route', 240).toUpperCase(),
    clearance: textField('clearance', 240).toUpperCase(),
    alternate: textField('alternate', 16).toUpperCase(),
    alternateRoute: textField('alternateRoute', 180).toUpperCase(),
    flightRules: textField('flightRules', 16).toUpperCase() || 'VFR',
    cruiseFlightLevel: textField('cruiseFlightLevel', 12).toUpperCase(),
    cruiseSpeedKt: numberField('cruiseSpeedKt', 0, 300),
    distanceNm: numberField('distanceNm', 0, 3000),
    estimatedEnrouteMinutes: numberField('estimatedEnrouteMinutes', 0, 1440),
    estimatedOut: textField('estimatedOut', 24).toUpperCase(),
    estimatedOff: textField('estimatedOff', 24).toUpperCase(),
    estimatedOn: textField('estimatedOn', 24).toUpperCase(),
    estimatedIn: textField('estimatedIn', 24).toUpperCase(),
    scheduledOut: textField('scheduledOut', 24).toUpperCase(),
    scheduledOff: textField('scheduledOff', 24).toUpperCase(),
    scheduledOn: textField('scheduledOn', 24).toUpperCase(),
    scheduledIn: textField('scheduledIn', 24).toUpperCase(),
    actualOut: textField('actualOut', 24).toUpperCase(),
    actualOff: textField('actualOff', 24).toUpperCase(),
    actualOn: textField('actualOn', 24).toUpperCase(),
    actualIn: textField('actualIn', 24).toUpperCase(),
    fuelFlowGph: numberField('fuelFlowGph', 0, 40),
    taxiFuelGal: numberField('taxiFuelGal', 0, 100),
    tripFuelGal: numberField('tripFuelGal', 0, 100),
    contingencyFuelGal: numberField('contingencyFuelGal', 0, 100),
    alternateFuelGal: numberField('alternateFuelGal', 0, 100),
    finalReserveFuelGal: numberField('finalReserveFuelGal', 0, 100),
    extraFuelGal: numberField('extraFuelGal', 0, 100),
    alternateDistanceNm: numberField('alternateDistanceNm', 0, 1000),
    alternateEetMinutes: numberField('alternateEetMinutes', 0, 1440),
    bemLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.basicEmptyWeightLbs,
    bemArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.basicEmptyCgIn,
    crew1Lb: numberField('crew1Lb', 0, 600),
    crew2Lb: numberField('crew2Lb', 0, 600),
    passengerWeightOverrideLb: optionalNumberField('passengerWeightOverrideLb', 0, 1200),
    baggageWeightOverrideLb: optionalNumberField('baggageWeightOverrideLb', 0, 600),
    crewArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.cockpit.armIn,
    passengerArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.rearSeats.armIn,
    fuelArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.fuel.armIn,
    baggageArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.baggage.armIn,
    baggageLimitLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.baggage.weightLimitLbs,
    fuelCapacityLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.fuel.weightLimitLbs,
    fuelCapacityGal: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.fuel.usableGallons,
    gearRetractionMomentChangeInLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.gearRetractionMomentChangeInLb,
    maxZeroFuelWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxZeroFuelWeightLbs,
    maxRampWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxRampWeightLbs,
    maxTakeoffWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxTakeoffWeightLbs,
    maxLandingWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxLandingWeightLbs,
    cgForwardLimitIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.forwardCgLimits.at(-1).cgIn,
    cgAftLimitIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.aftCgLimits.at(-1).cgIn,
    weightBalanceProfile: AIRCRAFT_WEIGHT_BALANCE_PROFILE,
    performanceSnapshot: normalizeOperationalPerformanceSnapshot(input.performanceSnapshot),
    notamReviewAccepted: input.notamReviewAccepted === true || input.notamReviewAccepted === 'true',
    airspaceReviewAccepted: input.airspaceReviewAccepted === true || input.airspaceReviewAccepted === 'true',
    briefingRemarks: textField('briefingRemarks', 500),
    briefingReviewedAt: textField('briefingReviewedAt', 40),
    stabTrim: textField('stabTrim', 20).toUpperCase(),
    releaseName: textField('releaseName', 60).toUpperCase(),
    releaseAccepted: input.releaseAccepted === true || input.releaseAccepted === 'true',
    releaseSnapshot: input.releaseSnapshot && typeof input.releaseSnapshot === 'object' ? input.releaseSnapshot : null
  };
  const calculations = operationalFlightPlanCalculations(plan, request);
  return { ...plan, ...calculations };
}

function normalizeBookingEmail(input) {
  return String(input || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9@._+\-]/g, '')
    .slice(0, 120);
}

function normalizeBookingSeats(input) {
  const seats = Number(input);
  if (!Number.isFinite(seats)) return 1;
  return Math.max(1, Math.min(3, Math.round(seats)));
}

function readWalletCredential(base64Name, pathName) {
  if (process.env[base64Name]) return Buffer.from(process.env[base64Name], 'base64');
  if (process.env[pathName]) return fs.readFileSync(process.env[pathName]);
  return null;
}

function getWalletCertificates() {
  const signerCert = readWalletCredential('WALLET_SIGNER_CERT_BASE64', 'WALLET_SIGNER_CERT_PATH');
  const signerKey = readWalletCredential('WALLET_SIGNER_KEY_BASE64', 'WALLET_SIGNER_KEY_PATH');
  const wwdr = readWalletCredential('WALLET_WWDR_BASE64', 'WALLET_WWDR_PATH');
  const passTypeIdentifier = String(process.env.WALLET_PASS_TYPE_IDENTIFIER || '').trim();
  const teamIdentifier = String(process.env.WALLET_TEAM_IDENTIFIER || '').trim();
  if (!signerCert || !signerKey || !wwdr || !passTypeIdentifier || !teamIdentifier) return null;
  return {
    signerCert,
    signerKey,
    wwdr,
    signerKeyPassphrase: process.env.WALLET_SIGNER_KEY_PASSPHRASE || undefined,
    passTypeIdentifier,
    teamIdentifier,
    organizationName: String(process.env.WALLET_ORGANIZATION_NAME || 'NatGlobe Aviation').trim()
  };
}

function walletAsset(name) {
  return fs.readFileSync(path.join(WALLET_ASSETS_DIR, name));
}

function createBoardingPassToken() {
  return randomUUID().replace(/-/g, '');
}

function createAgreementToken() {
  return randomUUID().replace(/-/g, '');
}

function createReimbursementStatementToken() {
  return randomUUID().replace(/-/g, '');
}

function createTimeProposalToken() {
  return randomUUID().replace(/-/g, '');
}

function createBookingManagementToken() {
  return randomUUID().replace(/-/g, '');
}

function getRequestPassengers(request) {
  return request.passengers?.length ? request.passengers : [{ number: 1, name: request.name }];
}

function ensureAgreementTokens(request) {
  let changed = false;
  getRequestPassengers(request).forEach(passenger => {
    if (!passenger.agreementToken) {
      passenger.agreementToken = createAgreementToken();
      changed = true;
    }
  });
  return changed;
}

function ensureReimbursementStatementToken(request) {
  if (request.reimbursementStatementToken) return false;
  request.reimbursementStatementToken = createReimbursementStatementToken();
  return true;
}

function isBoardingPassReady(request) {
  return request.pilotDecision === 'APPROVED' && request.status === 'CONFIRMED';
}

function estimateBoardingPassFlightTime(depIcao, arrIcao) {
  const minutes = estimateBoardingPassFlightMinutes(depIcao, arrIcao);
  if (minutes === null) return 'TBD';
  return `${String(Math.floor(minutes / 60)).padStart(2, '0')}.${String(minutes % 60).padStart(2, '0')}`;
}

function estimateBoardingPassFlightMinutes(depIcao, arrIcao) {
  const depAirport = bookingAirports.find(airport => airport.icao === depIcao);
  const arrAirport = bookingAirports.find(airport => airport.icao === arrIcao);
  if (!depAirport || !arrAirport) return null;
  if (depAirport.icao === 'EFHN' && arrAirport.icao === 'EFHN') return HANKO_SCENIC_DURATION_MIN;
  if (['EFHV', 'EFNU'].includes(depAirport.icao) && depAirport.icao === arrAirport.icao) return HELSINKI_CITY_SCENIC_DURATION_MIN;
  return Math.max(10, Math.round((kmToNm(haversineKm(depAirport, arrAirport)) / AIRCRAFT_PROFILE.cruiseTasKt) * 60));
}

function boardingPassAirportLabel(icao, fallback) {
  const airport = bookingAirports.find(item => item.icao === icao);
  const displayNames = { Hyvinkaa: 'Hyvinkää', Pyhtaa: 'Pyhtää' };
  const place = displayNames[airport?.city] || airport?.city || airport?.name || fallback || icao || 'TBD';
  return `${place} (${airport?.short || icao || '---'})`;
}

function maskedPassport(passenger) {
  const countryCodes = { Finland: 'FIN', Estonia: 'EST', Sweden: 'SWE', Norway: 'NOR', Denmark: 'DNK', Germany: 'DEU', France: 'FRA', Spain: 'ESP', Italy: 'ITA', Netherlands: 'NLD', Poland: 'POL', 'United Kingdom': 'GBR', Ireland: 'IRL', 'United States': 'USA', Canada: 'CAN' };
  const raw = String(passenger.nationalId || '').replace(/\s/g, '');
  const visiblePart = raw.length > 4 ? raw.slice(0, -4) : '';
  const country = countryCodes[passenger.passportCountry] || String(passenger.passportCountry || 'PPT').slice(0, 3).toUpperCase();
  return visiblePart ? `${country}/${visiblePart}****` : `${country}/VERIFIED`;
}

function boardingPassGate(icao) {
  return BOOKING_TERMINALS[icao] || 'GA STAND';
}

function boardingPassStatus(request) {
  const departure = new Date(`${request.requestDate || ''}T${request.requestTime || '00:00'}:00`);
  if (!Number.isNaN(departure.getTime()) && Date.now() > departure.getTime() + (4 * 60 * 60 * 1000)) {
    return { label: 'COMPLETED', tone: 'completed' };
  }
  if (request.status === 'DECLINED') return { label: 'DECLINED', tone: 'declined' };
  if (request.status === 'NEEDS INFO') return { label: 'PENDING', tone: 'pending' };
  return { label: 'CONFIRMED', tone: 'confirmed' };
}

function formatBoardingPassDate(value) {
  const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return match ? `${match[3]}.${match[2]}.${match[1]}` : (value || 'TBD');
}

function formatBoardingPassTime(value) {
  const match = String(value || '').match(/^(\d{1,2}):(\d{2})$/);
  return match ? `${String(match[1]).padStart(2, '0')}.${match[2]}` : (value || 'TBD');
}

function boardingPassBoardingTime(departureTime) {
  const match = String(departureTime || '').match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return 'TBD';
  const minutes = ((Number(match[1]) * 60) + Number(match[2]) - 20 + (24 * 60)) % (24 * 60);
  return `${String(Math.floor(minutes / 60)).padStart(2, '0')}.${String(minutes % 60).padStart(2, '0')}`;
}

function boardingPassArrivalTime(departureTime, depIcao, arrIcao) {
  const match = String(departureTime || '').match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return 'TBD';
  const depAirport = bookingAirports.find(airport => airport.icao === depIcao);
  const arrAirport = bookingAirports.find(airport => airport.icao === arrIcao);
  if (!depAirport || !arrAirport) return 'TBD';
  const minutes = estimateBoardingPassFlightMinutes(depIcao, arrIcao) || HANKO_SCENIC_DURATION_MIN;
  const total = (Number(match[1]) * 60 + Number(match[2]) + minutes) % (24 * 60);
  return `${String(Math.floor(total / 60)).padStart(2, '0')}.${String(total % 60).padStart(2, '0')}`;
}

function assignedPassengerSeat(request, passenger) {
  const selected = String(request.seatPreference || '').trim().toUpperCase();
  if (selected && selected !== 'NO PREFERENCE') return selected;
  return Number(passenger.number || 1) % 2 === 0 ? 'REAR RIGHT' : 'REAR LEFT';
}

function boardingPassBaggage(request) {
  if (request.carryOnBags !== 'YES') return 'NO BAGGAGE DECLARED';
  return `${request.bagType || 'CARRY-ON'} / ${request.baggageWeightKg || '0'} KG`;
}

function icsEscape(value) {
  return String(value || '').replace(/\\/g, '\\\\').replace(/;/g, '\\;').replace(/,/g, '\\,').replace(/\r?\n/g, '\\n');
}

function bookingCalendarDateTime(date, time, addMinutes = 0) {
  const match = `${date || ''}T${time || ''}`.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{1,2}):(\d{2})$/);
  if (!match) return null;
  const stamp = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]), Number(match[4]), Number(match[5]) + addMinutes));
  return `${stamp.getUTCFullYear()}${String(stamp.getUTCMonth() + 1).padStart(2, '0')}${String(stamp.getUTCDate()).padStart(2, '0')}T${String(stamp.getUTCHours()).padStart(2, '0')}${String(stamp.getUTCMinutes()).padStart(2, '0')}00`;
}

function buildBoardingPassCalendar(request, passenger) {
  const start = bookingCalendarDateTime(request.requestDate, request.requestTime);
  const duration = estimateBoardingPassFlightMinutes(request.dep, request.arr) || 60;
  const end = bookingCalendarDateTime(request.requestDate, request.requestTime, duration);
  if (!start || !end) return null;
  const route = `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`;
  return [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//Private Flight//Passenger Pass//EN',
    'CALSCALE:GREGORIAN',
    'BEGIN:VEVENT',
    `UID:${icsEscape(`${request.id}-${passenger.number || 1}@privateflight`)}`,
    `DTSTAMP:${new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '')}`,
    `DTSTART;TZID=Europe/Helsinki:${start}`,
    `DTEND;TZID=Europe/Helsinki:${end}`,
    `SUMMARY:${icsEscape(`Private Flight ${request.id}`)}`,
    `LOCATION:${icsEscape(boardingPassGate(request.dep))}`,
    `DESCRIPTION:${icsEscape(`${route}\nPassenger: ${passenger.name || 'PRIVATE GUEST'}\nSeat: ${assignedPassengerSeat(request, passenger)}\nBaggage: ${boardingPassBaggage(request)}`)}`,
    'END:VEVENT',
    'END:VCALENDAR',
    ''
  ].join('\r\n');
}

function publicBoardingPassView(request, passenger) {
  const title = ['MR', 'MS', 'MX', 'DR'].includes(String(passenger.title || '').toUpperCase())
    ? `${String(passenger.title).toUpperCase()}. `
    : '';
  const passStatus = boardingPassStatus(request);
  return {
    reference: request.id,
    passenger: `${title}${passenger.name || 'PRIVATE GUEST'}`,
    passengerNumber: passenger.number || 1,
    from: boardingPassAirportLabel(request.dep, request.depName),
    to: boardingPassAirportLabel(request.arr, request.arrName),
    date: formatBoardingPassDate(request.requestDate),
    boardingTime: boardingPassBoardingTime(request.requestTime),
    flightTime: estimateBoardingPassFlightTime(request.dep, request.arr),
    aircraft: 'PA28-200R',
    seat: assignedPassengerSeat(request, passenger),
    gate: boardingPassGate(request.dep),
    passport: maskedPassport(passenger),
    dob: formatBoardingPassDate(passenger.dob),
    departureTime: formatBoardingPassTime(request.requestTime),
    arrivalTime: boardingPassArrivalTime(request.requestTime, request.dep, request.arr),
    baggage: boardingPassBaggage(request),
    checkIn: 'ARRIVE 20 MIN BEFORE DEPARTURE TIME',
    status: passStatus.label,
    statusTone: passStatus.tone
  };
}

async function createWalletPass(request, passenger) {
  const certificates = getWalletCertificates();
  if (!certificates) {
    const error = new Error('APPLE WALLET SIGNING NOT CONFIGURED');
    error.code = 'WALLET_NOT_CONFIGURED';
    throw error;
  }

  const { PKPass } = await import('passkit-generator');
  const serialNumber = `NG-${request.id}-${passenger.number || 1}`.replace(/[^A-Z0-9-]/gi, '').toUpperCase();
  const barcodeMessage = JSON.stringify({
    reference: request.id,
    passenger: passenger.number || 1,
    route: request.route,
    date: request.requestDate
  });
  const passProps = {
    formatVersion: 1,
    passTypeIdentifier: certificates.passTypeIdentifier,
    teamIdentifier: certificates.teamIdentifier,
    serialNumber,
    organizationName: certificates.organizationName,
    description: 'Private flight boarding pass',
    logoText: 'PRIVATE FLIGHT',
    foregroundColor: 'rgb(255, 255, 255)',
    backgroundColor: 'rgb(0, 0, 0)',
    labelColor: 'rgb(102, 255, 153)',
    groupingIdentifier: `NATGLOBE-${request.id}`,
    suppressStripShine: true,
    barcodes: [{
      format: 'PKBarcodeFormatQR',
      message: barcodeMessage,
      messageEncoding: 'iso-8859-1',
      altText: request.id
    }],
    boardingPass: {
      transitType: 'PKTransitTypeAir',
      headerFields: [{ key: 'flight', label: 'PRIVATE FLIGHT', value: request.id }],
      primaryFields: [
        { key: 'from', label: 'FROM', value: request.depName || request.dep || 'DEPARTURE' },
        { key: 'to', label: 'TO', value: request.arrName || request.arr || 'ARRIVAL' }
      ],
      secondaryFields: [
        { key: 'passenger', label: 'PASSENGER', value: passenger.name || 'PRIVATE GUEST' },
        { key: 'date', label: 'DATE', value: request.requestDate || 'TBD' }
      ],
      auxiliaryFields: [
        { key: 'boarding', label: 'BOARDING', value: boardingPassBoardingTime(request.requestTime) },
        { key: 'aircraft', label: 'AIRCRAFT', value: PUBLIC_AIRCRAFT_TYPE }
      ],
      backFields: [
        { key: 'reference', label: 'BOOKING REFERENCE', value: request.id },
        { key: 'luggage', label: 'LUGGAGE', value: request.carryOnBags === 'YES' ? `${request.bagType || 'DECLARED'} / ${request.baggageWeightKg || '0'} KG` : 'NO LUGGAGE DECLARED' },
        { key: 'notice', label: 'IMPORTANT', value: 'Private flight request. Final operation remains subject to pilot decision, weather, aircraft serviceability, and applicable rules.' }
      ]
    }
  };
  const pass = new PKPass({
    'pass.json': Buffer.from(JSON.stringify(passProps)),
    'icon.png': walletAsset('icon.png'),
    'icon@2x.png': walletAsset('icon@2x.png'),
    'icon@3x.png': walletAsset('icon@3x.png'),
    'logo.png': walletAsset('logo.png'),
    'logo@2x.png': walletAsset('logo@2x.png')
  }, {
    wwdr: certificates.wwdr,
    signerCert: certificates.signerCert,
    signerKey: certificates.signerKey,
    signerKeyPassphrase: certificates.signerKeyPassphrase
  }, {});
  return { buffer: pass.getAsBuffer(), serialNumber };
}

function publicFlightView(flight) {
  const bookedSeats = bookingRequests
    .filter(req => req.flightId === flight.id && req.status !== 'CANCELLED')
    .reduce((sum, req) => sum + req.seats, 0);
  const seatsAvailable = Math.max(0, flight.seatsAvailable - bookedSeats);

  return {
    ...flight,
    seatsAvailable,
    bookingMode: seatsAvailable > 0 ? 'REQUEST AVAILABLE' : 'WAITLIST'
  };
}

async function getBookingAirportCatalog() {
  return bookingAirports;
}

async function getBookingAirport(icao) {
  const normalized = normalizeIcao(icao);
  const catalog = await getBookingAirportCatalog();
  return catalog.find(airport => airport.icao === normalized) || null;
}

function estimateBookingPrice(depAirport, arrAirport, seats, tripType) {
  if (!depAirport || !arrAirport) {
    return { perPassengerEur: 'TBD', totalEur: 'TBD', note: 'PILOT CONFIRMS' };
  }

  const roundtrip = tripType === 'ROUNDTRIP';
  const fixed = FIXED_BOOKING_ROUTE_PRICES[`${depAirport.icao}-${arrAirport.icao}`];
  if (fixed) {
    const perPassengerEur = roundtrip ? fixed.roundtrip : fixed.oneWay;
    return {
      perPassengerEur,
      totalEur: perPassengerEur * Math.max(1, seats),
      note: fixed.label
    };
  }

  return { perPassengerEur: 'ON REQUEST', totalEur: 'ON REQUEST', note: 'PRICE DETERMINED BY REQUEST' };
}

function normalizeBookingItineraryUpdate(body, fallbackTripType = 'ONE_WAY') {
  const depAirport = bookingAirports.find(airport => airport.icao === normalizeIcao(body?.dep));
  const arrAirport = bookingAirports.find(airport => airport.icao === normalizeIcao(body?.arr));
  if (!depAirport || !arrAirport) return { error: 'VALID DEPARTURE AND DESTINATION REQUIRED' };

  const tripType = body?.tripType === 'ROUNDTRIP' ? 'ROUNDTRIP' : fallbackTripType === 'ROUNDTRIP' ? 'ROUNDTRIP' : 'ONE_WAY';
  const flightExperience = normalizeBookingText(body?.flightExperience, 40);
  const isHankoScenic = flightExperience === HANKO_SCENIC_EXPERIENCE && depAirport.icao === 'EFHN' && arrAirport.icao === 'EFHN';
  const isHelsinkiCityScenic = ['EFHV', 'EFNU'].includes(depAirport.icao) && depAirport.icao === arrAirport.icao && (!flightExperience || flightExperience === HELSINKI_CITY_SCENIC_EXPERIENCE);
  if (depAirport.icao === arrAirport.icao && !isHankoScenic && !isHelsinkiCityScenic) return { error: 'CHOOSE DIFFERENT DEPARTURE AND DESTINATION OR SELECT A SCENIC FLIGHT' };

  const normalizedExperience = isHankoScenic ? HANKO_SCENIC_EXPERIENCE : isHelsinkiCityScenic ? HELSINKI_CITY_SCENIC_EXPERIENCE : '';
  const requestedScenicVariant = normalizeBookingText(body?.scenicVariant, 10).toUpperCase();
  const scenicVariant = isHelsinkiCityScenic
    ? (requestedScenicVariant === HELSINKI_NIGHT_VARIANT ? HELSINKI_NIGHT_VARIANT : HELSINKI_DAY_VARIANT)
    : '';
  const normalizedTripType = normalizedExperience ? 'ONE_WAY' : tripType;
  const title = isHankoScenic
    ? 'Hanko Regatta scenic overflight'
    : isHelsinkiCityScenic
      ? `Helsinki ${scenicVariant === HELSINKI_NIGHT_VARIANT ? 'night' : 'day'} scenic flight`
      : `${depAirport.city} to ${arrAirport.city}`;

  return {
    depAirport,
    arrAirport,
    tripType: normalizedTripType,
    flightExperience: normalizedExperience,
    scenicVariant,
    flightTitle: title,
    estimatedFlightMinutes: estimateBoardingPassFlightMinutes(depAirport.icao, arrAirport.icao) || null
  };
}

function applyBookingItineraryUpdate(request, update) {
  const previousRoute = `${request.dep || '---'}-${request.arr || '---'}`;
  const previousTitle = request.flightTitle || previousRoute;
  const priceEstimate = estimateBookingPrice(update.depAirport, update.arrAirport, request.seats || 1, update.tripType);

  request.flightTitle = update.flightTitle;
  request.route = `${update.depAirport.icao}-${update.arrAirport.icao}`;
  request.dep = update.depAirport.icao;
  request.arr = update.arrAirport.icao;
  request.depName = update.depAirport.name;
  request.arrName = update.arrAirport.name;
  request.tripType = update.tripType;
  request.flightExperience = update.flightExperience;
  request.scenicVariant = update.scenicVariant;
  request.costPerSeatEur = priceEstimate.perPassengerEur;
  request.estimatedTotalEur = priceEstimate.totalEur;
  request.priceNote = priceEstimate.note;
  request.estimatedFlightMinutes = update.estimatedFlightMinutes;

  if (update.flightExperience && request.returnDate) {
    request.returnDate = '';
    request.returnTime = '';
    request.returnPlan = '';
  }

  return `${previousTitle} / ${previousRoute} -> ${request.flightTitle} / ${request.route}`;
}

function formatBookingMessage(request) {
  const passengerLines = (request.passengers?.length ? request.passengers : [{
    number: 1,
    name: request.name,
    weightKg: request.weightKg,
    dob: request.dob,
    nationalId: request.nationalId,
    email: request.email,
    phone: request.phone
  }]).flatMap(passenger => [
    `PAX${passenger.number || 1} ${String(passenger.name || 'NIL').toUpperCase()}   WT ${passenger.weightKg || 'NIL'}KG`,
    `DOB ${passenger.dob || 'NIL'}   PPT CTRY ${passenger.passportCountry || 'NIL'}   ID ${passenger.nationalId || 'NIL'}   TEL ${passenger.phone || 'NIL'}   EMAIL ${passenger.email || 'NIL'}`
  ]);

  const bagType = String(request.bagType || '').replace(/\s*\/\s*$/, '').trim();
  const extras = String(request.extras || '').trim();
  const extrasNote = String(request.extrasNotes || '').trim();
  const emergency = request.emergencyName || request.emergencyPhone
    ? `EMERG ${request.emergencyName || 'NIL'} / ${request.emergencyPhone || 'NIL'}`
    : null;
  const returnLine = request.tripType === 'ROUNDTRIP'
    ? `RETURN ${request.returnDate || 'DATE TBD'} ${request.returnTime || 'TIME TBD'}   TRAVELLERS ${request.returnPlan || request.returnFlexibility || 'TO BE CONFIRMED'}`
    : null;
  return [
    'PRIVATE FLIGHT OPERATIONS REQUEST',
    '------------------------',
    `REF ${request.id}   STATUS ${request.status}`,
    `FLT ${request.flightId}   ${request.dep}-${request.arr}   ${request.requestDate} ${request.requestTime}`,
    `TITLE ${String(request.flightTitle || 'CUSTOM ROUTE').toUpperCase()}`,
    `A/C ${String(request.aircraft || AIRCRAFT_PROFILE.registration + ' / ' + AIRCRAFT_PROFILE.type).toUpperCase()}`,
    `CREW CMD ${request.crew?.commander || 'UNASSIGNED'}   SIC ${request.crew?.secondary || 'NONE'}`,
    `PAX COUNT ${request.seats}`,
    ...passengerLines,
    emergency,
    returnLine,
    request.carryOnBags === 'YES' ? `BAG YES   WT ${request.baggageWeightKg || '0'}KG   PWRBANK ${request.powerBanks || 'NO'}` : null,
    request.carryOnBags === 'YES' && bagType ? `BAG TYPE ${bagType}` : null,
    request.seatPreference && request.seatPreference !== 'NO PREFERENCE' ? `SEAT PREF ${request.seatPreference}` : null,
    extras ? `EXTRAS ${extras}${extrasNote ? `   RMK ${extrasNote}` : ''}` : null,
    `TRIP ${request.tripType === 'ROUNDTRIP' ? 'ROUNDTRIP' : 'ONE WAY'}   PRICE EUR ${request.costPerSeatEur || 'TBD'} / PAX   TOTAL EUR ${request.estimatedTotalEur || 'TBD'}`,
    `PRICE NOTE ${request.priceNote || 'PILOT CONFIRMS FINAL PRICE'}`,
    `PILOT DECISION ${request.pilotDecision || 'PENDING'}   PAYMENT ${request.paymentStatus || 'UNPAID'}`,
    request.message ? `RMK ${request.message}` : null,
    'END OF MESSAGE'
  ].filter(Boolean).join('\n');
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

function parseCsv(csvText) {
  const rows = [];
  let row = [];
  let cell = '';
  let inQuotes = false;

  for (let i = 0; i < csvText.length; i++) {
    const ch = csvText[i];
    const next = csvText[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === ',' && !inQuotes) {
      row.push(cell);
      cell = '';
      continue;
    }

    if ((ch === '\n' || ch === '\r') && !inQuotes) {
      if (ch === '\r' && next === '\n') i++;
      row.push(cell);
      if (row.some(value => value !== '')) rows.push(row);
      row = [];
      cell = '';
      continue;
    }

    cell += ch;
  }

  if (cell.length || row.length) {
    row.push(cell);
    if (row.some(value => value !== '')) rows.push(row);
  }

  if (!rows.length) return [];

  const headers = rows[0];
  return rows.slice(1).map(cols => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = cols[i] ?? '';
    }
    return obj;
  });
}

function normalizeAirportRow(row) {
  const icao = String(row.gps_code || row.ident || '')
    .trim()
    .toUpperCase();

  const lat = Number(row.latitude_deg);
  const lon = Number(row.longitude_deg);
  const elevationFt = Number(row.elevation_ft);
  const country = String(row.iso_country || '').trim().toUpperCase();

  if (!/^[A-Z]{4}$/.test(icao)) return null;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (!EUROPE_COUNTRIES.has(country)) return null;

  return {
    icao,
    name: String(row.name || '').trim(),
    municipality: String(row.municipality || '').trim(),
    country,
    type: String(row.type || '').trim(),
    lat,
    lon,
    elevationFt: Number.isFinite(elevationFt) ? elevationFt : null
  };
}

function getBundledAirportFallback() {
  return (bundledAirports || [])
    .filter(a => a && /^[A-Z]{4}$/.test(String(a.icao || '').toUpperCase()))
    .filter(a => Number.isFinite(Number(a.lat)) && Number.isFinite(Number(a.lon)))
    .map(a => ({
      icao: String(a.icao).toUpperCase(),
      name: String(a.name || '').trim(),
      municipality: String(a.city || '').trim(),
      country: String(a.country || '').trim().toUpperCase(),
      type: String(a.type || 'airport').trim(),
      lat: Number(a.lat),
      lon: Number(a.lon),
      elevationFt: Number.isFinite(Number(a.elevationFt)) ? Number(a.elevationFt) : null
    }))
    .filter(a => EUROPE_COUNTRIES.has(a.country));
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 5000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchCsvAirports(url) {
  const res = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
    FETCH_TIMEOUT_AIRPORT_DB_MS
  );

  if (!res.ok) {
    throw new Error(`AIRPORT DB HTTP ${res.status}`);
  }

  const csv = await res.text();
  return parseCsv(csv)
    .map(normalizeAirportRow)
    .filter(Boolean);
}

function normalizeRunwayEndIdent(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '');
}

function normalizeSurface(value) {
  return String(value || '')
    .trim()
    .toUpperCase();
}

function roundMaybe(value, digits = 2) {
  if (!Number.isFinite(value)) return null;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function isValidRunwayIdent(value) {
  return RUNWAY_IDENT_RE.test(String(value || '').trim());
}

function computeDirectionalSlopePct(startElevationFt, oppositeElevationFt, lengthFt, fallbackSlopePct = null) {
  if (
    Number.isFinite(startElevationFt) &&
    Number.isFinite(oppositeElevationFt) &&
    Number.isFinite(lengthFt) &&
    lengthFt > 0
  ) {
    return roundMaybe(((oppositeElevationFt - startElevationFt) / lengthFt) * 100, 2);
  }

  return Number.isFinite(fallbackSlopePct) ? roundMaybe(fallbackSlopePct, 2) : null;
}

function normalizeRunwayRow(row) {
  const airportIcao = normalizeIcao(row.airport_ident || '');
  if (!airportIcao) return null;

  const lengthFt = Number(row.length_ft);
  const widthFt = Number(row.width_ft);
  const csvSlopePct = Number(row.slope);
  const surface = normalizeSurface(row.surface);

  const leIdent = normalizeRunwayEndIdent(row.le_ident);
  const heIdent = normalizeRunwayEndIdent(row.he_ident);

  const leHeading = Number(row.le_heading_degT);
  const heHeading = Number(row.he_heading_degT);

  const leElevationFt = Number(row.le_elevation_ft);
  const heElevationFt = Number(row.he_elevation_ft);

  const leDisplacedFt = Number(row.le_displaced_threshold_ft);
  const heDisplacedFt = Number(row.he_displaced_threshold_ft);

  const normalizedLengthFt = Number.isFinite(lengthFt) ? Math.round(lengthFt) : null;
  const normalizedCsvSlopePct = Number.isFinite(csvSlopePct) ? roundMaybe(csvSlopePct, 2) : null;

  const le = isValidRunwayIdent(leIdent) ? {
    ident: leIdent,
    headingDegT: Number.isFinite(leHeading) ? Math.round(leHeading) : null,
    elevationFt: Number.isFinite(leElevationFt) ? Math.round(leElevationFt) : null,
    displacedThresholdFt: Number.isFinite(leDisplacedFt) ? Math.round(leDisplacedFt) : 0
  } : null;

  const he = isValidRunwayIdent(heIdent) ? {
    ident: heIdent,
    headingDegT: Number.isFinite(heHeading) ? Math.round(heHeading) : null,
    elevationFt: Number.isFinite(heElevationFt) ? Math.round(heElevationFt) : null,
    displacedThresholdFt: Number.isFinite(heDisplacedFt) ? Math.round(heDisplacedFt) : 0
  } : null;

  return {
    airportIcao,
    airportRef: String(row.airport_ref || '').trim(),
    lengthFt: normalizedLengthFt,
    widthFt: Number.isFinite(widthFt) ? Math.round(widthFt) : null,
    surface,
    csvSlopePct: normalizedCsvSlopePct,
    le,
    he
  };
}

async function fetchCsvRunways(url) {
  const res = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
    FETCH_TIMEOUT_AIRPORT_DB_MS
  );

  if (!res.ok) {
    throw new Error(`RUNWAY DB HTTP ${res.status}`);
  }

  const csv = await res.text();
  return parseCsv(csv)
    .map(normalizeRunwayRow)
    .filter(Boolean);
}

async function loadAirportDatabase() {
  const now = Date.now();

  if (airportDbCache.data && now - airportDbCache.loadedAt < AIRPORT_DB_TTL_MS) {
    return airportDbCache.data;
  }

  if (airportDbCache.promise) {
    return airportDbCache.promise;
  }

  airportDbCache.promise = (async () => {
    try {
      const remoteAirports = await fetchCsvAirports(OURAIRPORTS_CSV_URL);
      const mergedMap = new Map();

      for (const apt of [...getBundledAirportFallback(), ...remoteAirports]) {
        if (!mergedMap.has(apt.icao)) mergedMap.set(apt.icao, apt);
      }

      const data = Array.from(mergedMap.values());
      airportDbCache.data = data;
      airportDbCache.loadedAt = Date.now();
      airportDbCache.promise = null;
      console.log(`Loaded airport DB: ${data.length} European aerodromes`);
      return data;
    } catch (err) {
      console.warn('Failed to load remote airport DB, using bundled fallback:', err.message);
      const data = getBundledAirportFallback();
      airportDbCache.data = data;
      airportDbCache.loadedAt = Date.now();
      airportDbCache.promise = null;
      return data;
    }
  })();

  return airportDbCache.promise;
}

async function loadRunwayDatabase() {
  const now = Date.now();

  if (runwayDbCache.data && now - runwayDbCache.loadedAt < AIRPORT_DB_TTL_MS) {
    return runwayDbCache.data;
  }

  if (runwayDbCache.promise) {
    return runwayDbCache.promise;
  }

  runwayDbCache.promise = (async () => {
    try {
      const airportDb = await loadAirportDatabase();
      const europeIcaoSet = new Set(airportDb.map(a => a.icao));

      const remoteRunways = await fetchCsvRunways(OURAIRPORTS_RUNWAYS_CSV_URL);
      const data = remoteRunways.filter(r => europeIcaoSet.has(r.airportIcao));

      runwayDbCache.data = data;
      runwayDbCache.loadedAt = Date.now();
      runwayDbCache.promise = null;
      console.log(`Loaded runway DB: ${data.length} European runway records`);
      return data;
    } catch (err) {
      console.warn('Failed to load runway DB:', err.message);
      const data = [];
      runwayDbCache.data = data;
      runwayDbCache.loadedAt = Date.now();
      runwayDbCache.promise = null;
      return data;
    }
  })();

  return runwayDbCache.promise;
}

async function findAirportByIcao(icao) {
  const airportDb = await loadAirportDatabase();
  return airportDb.find(a => a.icao === icao) || null;
}

function buildRunwayDirectionEntry(airport, parentRunway, end, oppositeEnd) {
  if (!end?.ident || !isValidRunwayIdent(end.ident)) return null;

  const totalLengthFt = Number.isFinite(parentRunway.lengthFt) ? parentRunway.lengthFt : null;
  const displacedFt = Number.isFinite(end.displacedThresholdFt) ? end.displacedThresholdFt : 0;

  const ldaFt = Number.isFinite(totalLengthFt)
    ? Math.max(0, totalLengthFt - displacedFt)
    : null;

  const elevationFt = Number.isFinite(end.elevationFt)
    ? end.elevationFt
    : (Number.isFinite(airport?.elevationFt) ? airport.elevationFt : null);

  const oppositeElevationFt = Number.isFinite(oppositeEnd?.elevationFt) ? oppositeEnd.elevationFt : null;

  return {
    airport: airport?.icao || parentRunway.airportIcao,
    ident: end.ident,
    heading: Number.isFinite(end.headingDegT) ? end.headingDegT : null,
    reciprocalIdent: oppositeEnd?.ident || '',
    lengthFt: totalLengthFt,
    toraFt: totalLengthFt,
    ldaFt,
    elevationFt,
    slopePct: computeDirectionalSlopePct(
      elevationFt,
      oppositeElevationFt,
      totalLengthFt,
      parentRunway.csvSlopePct
    ),
    surface: parentRunway.surface || '',
    widthFt: Number.isFinite(parentRunway.widthFt) ? parentRunway.widthFt : null
  };
}

async function getAirportRunways(icao) {
  const normalizedIcao = normalizeIcao(icao);
  if (!normalizedIcao) return [];

  const [airport, runwayDb] = await Promise.all([
    findAirportByIcao(normalizedIcao),
    loadRunwayDatabase()
  ]);

  if (!airport) return [];

  const deduped = new Map();

  for (const parentRunway of runwayDb.filter(r => r.airportIcao === normalizedIcao)) {
    const directions = [
      buildRunwayDirectionEntry(airport, parentRunway, parentRunway.le, parentRunway.he),
      buildRunwayDirectionEntry(airport, parentRunway, parentRunway.he, parentRunway.le)
    ].filter(Boolean);

    for (const direction of directions) {
      if (!deduped.has(direction.ident)) {
        deduped.set(direction.ident, direction);
      }
    }
  }

  return Array.from(deduped.values()).sort((a, b) => {
    const aNum = parseInt(String(a.ident).replace(/[^\d]/g, ''), 10);
    const bNum = parseInt(String(b.ident).replace(/[^\d]/g, ''), 10);

    if (Number.isFinite(aNum) && Number.isFinite(bNum) && aNum !== bNum) {
      return aNum - bNum;
    }

    return String(a.ident).localeCompare(String(b.ident));
  });
}

async function findRunwayDirection(icao, ident) {
  const runwayIdent = normalizeRunwayEndIdent(ident);
  if (!normalizeIcao(icao) || !runwayIdent) return null;

  const runways = await getAirportRunways(icao);
  return runways.find(r => r.ident === runwayIdent) || null;
}

async function getNearbyAirports(icao, limit = FALLBACK_SEARCH_LIMIT) {
  const airportDb = await loadAirportDatabase();
  const target = airportDb.find(a => a.icao === icao);

  if (!target) return [];

  return airportDb
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

async function fetchRaw(url) {
  const cacheKey = `raw:${url}`;
  const cached = getCached(cacheKey);
  if (cached !== undefined) return cached;

  try {
    const res = await fetchWithTimeout(
      url,
      { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
      FETCH_TIMEOUT_RAW_MS
    );

    if (res.status === 204) {
      setCached(cacheKey, null, WEATHER_CACHE_TTL_MS);
      return null;
    }

    if (!res.ok) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const text = String(await res.text() || '').trim() || null;
    setCached(cacheKey, text, WEATHER_CACHE_TTL_MS);
    return text;
  } catch {
    return null;
  }
}

async function getMetar(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/metar?ids=${icao}&format=raw`);
}

async function getTaf(icao) {
  return fetchRaw(`https://aviationweather.gov/api/data/taf?ids=${icao}&format=raw`);
}

async function getNearestOfficial(fetchFn, requestedIcao) {
  const nearby = await getNearbyAirports(requestedIcao, FALLBACK_SEARCH_LIMIT);

  for (let i = 0; i < nearby.length; i += FALLBACK_BATCH_SIZE) {
    const batch = nearby.slice(i, i + FALLBACK_BATCH_SIZE);

    const results = await Promise.all(
      batch.map(async (apt) => {
        const found = await fetchFn(apt.icao);
        if (!found) return null;

        return {
          text: found,
          source: apt.icao,
          fallback: true,
          distanceKm: apt.distanceKm,
          distanceNm: apt.distanceNm
        };
      })
    );

    const firstHit = results.find(Boolean);
    if (firstHit) return firstHit;
  }

  return {
    text: 'NOT AVAILABLE',
    source: null,
    fallback: false,
    distanceKm: null,
    distanceNm: null
  };
}

function parseWindKt(metar) {
  const gustMatch = metar.match(/\b(\d{3}|VRB)(\d{2,3})G(\d{2,3})KT\b/);
  if (gustMatch) {
    return {
      speed: parseInt(gustMatch[2], 10),
      gust: parseInt(gustMatch[3], 10)
    };
  }

  const basicMatch = metar.match(/\b(\d{3}|VRB)(\d{2,3})KT\b/);
  if (basicMatch) {
    return {
      speed: parseInt(basicMatch[2], 10),
      gust: null
    };
  }

  return null;
}

function parseWindDetailed(metar) {
  const gustMatch = String(metar || '').match(/\b(\d{3}|VRB)(\d{2,3})G(\d{2,3})KT\b/);
  if (gustMatch) {
    return {
      raw: gustMatch[0],
      direction: gustMatch[1] === 'VRB' ? null : parseInt(gustMatch[1], 10),
      speed: parseInt(gustMatch[2], 10),
      gust: parseInt(gustMatch[3], 10),
      variable: gustMatch[1] === 'VRB'
    };
  }

  const basicMatch = String(metar || '').match(/\b(\d{3}|VRB)(\d{2,3})KT\b/);
  if (basicMatch) {
    return {
      raw: basicMatch[0],
      direction: basicMatch[1] === 'VRB' ? null : parseInt(basicMatch[1], 10),
      speed: parseInt(basicMatch[2], 10),
      gust: null,
      variable: basicMatch[1] === 'VRB'
    };
  }

  return null;
}

function parseVisibility(metar) {
  const visMatch = metar.match(/\b(\d{4})\b/);
  if (!visMatch) return null;

  const value = parseInt(visMatch[1], 10);
  if (Number.isNaN(value)) return null;

  return value;
}

function parseWeatherPhenomena(metar) {
  const phenomena = [];
  const tokens = ['FG', 'BR', 'RA', 'SN', 'TS', 'DZ', 'FZRA', 'SHRA', 'SHSN'];

  for (const token of tokens) {
    if (metar.includes(token)) phenomena.push(token);
  }

  return phenomena;
}

function parseCloudBase(metar) {
  const cloudRegex = /\b(FEW|SCT|BKN|OVC)(\d{3})\b/g;
  let match;
  let lowest = null;

  while ((match = cloudRegex.exec(metar)) !== null) {
    const base = parseInt(match[2], 10) * 100;
    if (!Number.isNaN(base)) {
      if (lowest === null || base < lowest) lowest = base;
    }
  }

  return lowest;
}

function parseTemperatureC(metar) {
  const match = metar.match(/\b(M?\d{2})\/(M?\d{2})\b/);
  if (!match) return null;

  const raw = match[1];
  const isMinus = raw.startsWith('M');
  const num = parseInt(raw.replace('M', ''), 10);

  if (Number.isNaN(num)) return null;
  return isMinus ? -num : num;
}

function parseQnhHpa(metar) {
  const match = String(metar || '').match(/\bQ(\d{4})\b/);
  if (!match) return null;

  const value = parseInt(match[1], 10);
  return Number.isFinite(value) ? value : null;
}

function hpaToInHg(hpa) {
  if (!Number.isFinite(hpa)) return null;
  return Math.round((hpa * 0.0295299830714) * 100) / 100;
}

function computePressureAltitudeFt(elevationFt, qnhHpa) {
  if (!Number.isFinite(elevationFt) || !Number.isFinite(qnhHpa)) return null;
  return Math.round(elevationFt + ((1013.25 - qnhHpa) * 27));
}

function computeDensityAltitudeFt(elevationFt, oatC, qnhHpa) {
  if (!Number.isFinite(elevationFt) || !Number.isFinite(oatC) || !Number.isFinite(qnhHpa)) {
    return null;
  }

  const pressureAltitudeFt = computePressureAltitudeFt(elevationFt, qnhHpa);
  if (!Number.isFinite(pressureAltitudeFt)) return null;

  const isaTempC = 15 - (2 * (pressureAltitudeFt / 1000));
  return Math.round(pressureAltitudeFt + (120 * (oatC - isaTempC)));
}

function normalizeDegrees(deg) {
  if (!Number.isFinite(deg)) return null;
  let value = deg % 360;
  if (value < 0) value += 360;
  return value;
}

function computeWindComponents(windDirectionDeg, windSpeedKt, runwayHeadingDeg) {
  if (!Number.isFinite(windDirectionDeg) || !Number.isFinite(windSpeedKt) || !Number.isFinite(runwayHeadingDeg)) {
    return {
      headwindKt: null,
      crosswindKt: null
    };
  }

  const diffDeg = normalizeDegrees(windDirectionDeg - runwayHeadingDeg);
  const angleRad = (diffDeg * Math.PI) / 180;

  return {
    headwindKt: Math.round(windSpeedKt * Math.cos(angleRad)),
    crosswindKt: Math.round(Math.abs(windSpeedKt * Math.sin(angleRad)))
  };
}

function arrivalNeedsAlternate(wx) {
  if (!wx || !wx.metar || wx.metar === 'NOT AVAILABLE') return false;

  const metar = String(wx.metar);
  const wind = parseWindKt(metar);
  const vis = parseVisibility(metar);
  const phenomena = parseWeatherPhenomena(metar);
  const cloudBase = parseCloudBase(metar);

  if (vis !== null && vis < 5000) return true;
  if (cloudBase !== null && cloudBase <= 1000) return true;
  if (phenomena.includes('TS') || phenomena.includes('FG')) return true;
  if (wind?.gust && wind.gust >= 25) return true;

  return false;
}

function pushUnique(remarks, value) {
  if (!remarks.includes(value)) remarks.push(value);
}

function generateAutoRemark(data) {
  if (data.remarks) return data.remarks;

  const remarks = [];

  function inspect(wx) {
    if (!wx || !wx.metar || wx.metar === 'NOT AVAILABLE') return;

    const metar = String(wx.metar);
    const wind = parseWindKt(metar);
    const vis = parseVisibility(metar);
    const phenomena = parseWeatherPhenomena(metar);
    const cloudBase = parseCloudBase(metar);
    const tempC = parseTemperatureC(metar);

    if (vis !== null) {
      if (vis < 1500) {
        pushUnique(remarks, 'VERY LOW VIS');
      } else if (vis < 2000) {
        pushUnique(remarks, 'POOR VIS');
      } else if (vis < 5000) {
        pushUnique(remarks, 'LOW VIS');
      }
    }

    if (wind) {
      if (wind.gust && wind.gust >= 25) {
        pushUnique(remarks, `GUSTS ${wind.gust}KT`);
      } else if (wind.speed >= 20) {
        pushUnique(remarks, 'STRONG WIND');
      }

      if (wind.gust && wind.gust - wind.speed >= 10) {
        pushUnique(remarks, 'GUST SPREAD');
      }
    }

    if (cloudBase !== null) {
      if (cloudBase <= 500) {
        pushUnique(remarks, 'VERY LOW CEILING');
      } else if (cloudBase <= 2000) {
        pushUnique(remarks, 'LOW CEILING');
      }
    }

    if (phenomena.includes('TS')) {
      pushUnique(remarks, 'TS');
    }

    if (phenomena.includes('FG')) {
      pushUnique(remarks, 'FOG');
    } else if (phenomena.includes('BR')) {
      pushUnique(remarks, 'MIST');
    }

    if (phenomena.includes('FZRA')) {
      pushUnique(remarks, 'FREEZING PRECIP');
    } else if (
      phenomena.includes('RA') ||
      phenomena.includes('SHRA') ||
      phenomena.includes('DZ')
    ) {
      pushUnique(remarks, 'PRECIP');
    }

    if (phenomena.includes('SN') || phenomena.includes('SHSN')) {
      pushUnique(remarks, 'SNOW');
    }

    if (
      tempC !== null &&
      tempC <= 2 &&
      (
        phenomena.includes('RA') ||
        phenomena.includes('SHRA') ||
        phenomena.includes('DZ') ||
        phenomena.includes('FZRA') ||
        phenomena.includes('SN') ||
        phenomena.includes('SHSN') ||
        (cloudBase !== null && cloudBase <= 2000)
      )
    ) {
      pushUnique(remarks, 'ICING RISK');
    }
  }

  inspect(data.depWx);
  inspect(data.arrWx);

  if (arrivalNeedsAlternate(data.arrWx)) {
    pushUnique(remarks, 'ALTN REQUIRED');
  }

  return remarks.length ? remarks.join(' / ') : 'NIL';
}

async function getAirportWeather(icao) {
  if (!icao) return null;

  const [officialMetar, officialTaf] = await Promise.all([
    getMetar(icao),
    getTaf(icao)
  ]);

  let metar = officialMetar;
  let taf = officialTaf;
  let metarSource = officialMetar ? icao : null;
  let tafSource = officialTaf ? icao : null;
  let metarFallback = false;
  let tafFallback = false;
  let metarDistanceNm = 0;
  let tafDistanceNm = 0;
  let mode = 'LIVE';

  const fallbackPromises = [];
  let needFallbackMetar = false;
  let needFallbackTaf = false;

  if (!metar) {
    needFallbackMetar = true;
    fallbackPromises.push(getNearestOfficial(getMetar, icao));
  }

  if (!taf) {
    needFallbackTaf = true;
    fallbackPromises.push(getNearestOfficial(getTaf, icao));
  }

  if (fallbackPromises.length) {
    const results = await Promise.all(fallbackPromises);
    let resultIndex = 0;

    if (needFallbackMetar) {
      const fallbackMetar = results[resultIndex++];
      metar = fallbackMetar.text;
      metarSource = fallbackMetar.source;
      metarFallback = fallbackMetar.fallback;
      metarDistanceNm = fallbackMetar.distanceNm;
      if (fallbackMetar.fallback) mode = 'FALLBACK';
    }

    if (needFallbackTaf) {
      const fallbackTaf = results[resultIndex++];
      taf = fallbackTaf.text;
      tafSource = fallbackTaf.source;
      tafFallback = fallbackTaf.fallback;
      tafDistanceNm = fallbackTaf.distanceNm;
      if (fallbackTaf.fallback) mode = 'FALLBACK';
    }
  }

  if (!metar) metar = 'NOT AVAILABLE';
  if (!taf) taf = 'NOT AVAILABLE';

  if (metar === 'NOT AVAILABLE' && taf === 'NOT AVAILABLE') {
    mode = 'NO DATA';
  }

  return {
    airport: icao,
    mode,
    metar,
    taf,
    metarSource,
    tafSource,
    metarFallback,
    tafFallback,
    metarDistanceNm,
    tafDistanceNm
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

      if (word.length > maxChars) {
        for (let i = 0; i < word.length; i += maxChars) {
          lines.push(word.slice(i, i + maxChars));
        }
        current = '';
      } else {
        current = word;
      }
    }
  }

  if (current) lines.push(current);
  return lines.length ? lines : [''];
}

function formatDateOnly(d) {
  return d.toISOString().slice(0, 10);
}

function formatUtcClock(d) {
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${hh}:${mm} UTC`;
}

function pickNearestHourlyIndex(times) {
  if (!Array.isArray(times) || !times.length) return -1;

  const now = Date.now();
  let bestIndex = -1;
  let bestDiff = Infinity;

  for (let i = 0; i < times.length; i++) {
    const t = Date.parse(times[i]);
    if (Number.isNaN(t)) continue;

    const diff = t - now;
    if (diff >= 0 && diff < bestDiff) {
      bestDiff = diff;
      bestIndex = i;
    }
  }

  if (bestIndex !== -1) return bestIndex;
  return times.length - 1;
}

function formatWindAloft(directionDeg, speedKt) {
  if (!Number.isFinite(directionDeg) || !Number.isFinite(speedKt)) return null;

  let dir = Math.round(directionDeg / 10) * 10;
  if (dir === 360) dir = 0;

  const ddd = String(dir).padStart(3, '0');
  const ss = String(Math.round(speedKt)).padStart(2, '0');
  return `${ddd}${ss}KT`;
}

async function getWindsAloftForAirport(icao) {
  if (!icao) return null;

  const airport = await findAirportByIcao(icao);
  if (!airport || !Number.isFinite(airport.lat) || !Number.isFinite(airport.lon)) return null;

  const url = new URL('https://api.open-meteo.com/v1/gfs');
  url.searchParams.set('latitude', String(airport.lat));
  url.searchParams.set('longitude', String(airport.lon));
  url.searchParams.set(
    'hourly',
    [
      'wind_speed_925hPa',
      'wind_direction_925hPa',
      'wind_speed_850hPa',
      'wind_direction_850hPa',
      'wind_speed_700hPa',
      'wind_direction_700hPa'
    ].join(',')
  );
  url.searchParams.set('wind_speed_unit', 'kn');
  url.searchParams.set('timezone', 'UTC');
  url.searchParams.set('forecast_days', '2');

  const cacheKey = `winds:${url.toString()}`;
  const cached = getCached(cacheKey);
  if (cached !== undefined) return cached;

  try {
    const res = await fetchWithTimeout(
      url.toString(),
      { headers: { 'User-Agent': 'NatGlobeAviation/1.0' } },
      FETCH_TIMEOUT_WINDS_MS
    );

    if (!res.ok) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const json = await res.json();
    const hourly = json?.hourly;

    if (!hourly?.time?.length) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const idx = pickNearestHourlyIndex(hourly.time);
    if (idx < 0) {
      setCached(cacheKey, null, NEGATIVE_CACHE_TTL_MS);
      return null;
    }

    const formatted = {
      airport: icao,
      timeUtc: hourly.time[idx] || null,
      levels: {
        '2000FT': formatWindAloft(hourly.wind_direction_925hPa?.[idx], hourly.wind_speed_925hPa?.[idx]),
        'FL050': formatWindAloft(hourly.wind_direction_850hPa?.[idx], hourly.wind_speed_850hPa?.[idx]),
        'FL100': formatWindAloft(hourly.wind_direction_700hPa?.[idx], hourly.wind_speed_700hPa?.[idx])
      }
    };

    setCached(cacheKey, formatted, WEATHER_CACHE_TTL_MS);
    return formatted;
  } catch {
    return null;
  }
}

function hasAnyWindsLine(windsAloft) {
  if (!windsAloft) return false;

  const dep = windsAloft.dep?.levels || {};
  const arr = windsAloft.arr?.levels || {};

  return Object.values(dep).some(Boolean) || Object.values(arr).some(Boolean);
}

function buildWindsLine(label, winds) {
  if (!winds?.levels) return null;

  const parts = [];
  if (winds.levels['2000FT']) parts.push(`2000FT ${winds.levels['2000FT']}`);
  if (winds.levels['FL050']) parts.push(`FL050 ${winds.levels['FL050']}`);
  if (winds.levels['FL100']) parts.push(`FL100 ${winds.levels['FL100']}`);

  if (!parts.length) return null;
  return `${label} ${parts.join('   ')}`;
}

function buildDispatchData(query, depWx, arrWx, altnWx, windsAloft) {
  const now = new Date();

  return {
    date: formatDateOnly(now),
    timeUtc: formatUtcClock(now),
    flight: normalizeFlight(query.flight),
    route: normalizeRoute(query.route),
    remarks: normalizeRemarks(query.remarks),
    dep: normalizeIcao(query.dep),
    arr: normalizeIcao(query.arr),
    depWx,
    arrWx,
    altnWx,
    windsAloft,
    includeDepMetar: parseBoolean(query.includeDepMetar),
    includeDepTaf: parseBoolean(query.includeDepTaf),
    includeArrMetar: parseBoolean(query.includeArrMetar),
    includeArrTaf: parseBoolean(query.includeArrTaf),
    includeAltnMetar: parseBoolean(query.includeAltnMetar),
    includeAltnTaf: parseBoolean(query.includeAltnTaf),
    includeWindsAloft: parseBoolean(query.includeWindsAloft)
  };
}

function buildWeatherSectionLines(label, wx, wantMetar, wantTaf) {
  if (!wx || (!wantMetar && !wantTaf)) return [];

  const lines = [];
  lines.push(`${label} (${wx.airport})`);

  if (wantMetar) {
    if (wx.metarFallback && wx.metarSource) {
      lines.push('METAR MODE FALLBACK');
      lines.push(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`);
    } else if (wx.metar && wx.metar !== 'NOT AVAILABLE') {
      lines.push('METAR MODE LIVE');
    } else {
      lines.push('METAR MODE NO DATA');
    }

    lines.push(wx.metar || 'NOT AVAILABLE');
    lines.push('');
  }

  if (wantTaf) {
    if (wx.tafFallback && wx.tafSource) {
      lines.push('TAF MODE FALLBACK');
      lines.push(`TAF SRC ${wx.tafSource}/${wx.tafDistanceNm}NM`);
    } else if (wx.taf && wx.taf !== 'NOT AVAILABLE') {
      lines.push('TAF MODE LIVE');
    } else {
      lines.push('TAF MODE NO DATA');
    }

    lines.push(wx.taf || 'NOT AVAILABLE');
    lines.push('');
  }

  return lines;
}

function buildDispatchText(data) {
  const headerLine1 = [
    'OPS NATGLOBE AVIATION',
    `DATE ${data.date}`,
    `UTC ${data.timeUtc.replace(' UTC', '')}`
  ].join('   ');

  const headerLine2 = [
    data.flight ? `FLT ${data.flight}` : null,
    data.dep ? `ORIG ${data.dep}` : null,
    data.arr ? `DEST ${data.arr}` : null,
    data.route ? `ROUTE ${data.route}` : null
  ].filter(Boolean).join('   ');

  const lines = [
    'ACARS WEATHER REPORT',
    '--------------------',
    headerLine1,
    headerLine2,
    ''
  ];

  lines.push(...buildWeatherSectionLines('DEP WX', data.depWx, data.includeDepMetar, data.includeDepTaf));
  lines.push(...buildWeatherSectionLines('ARR WX', data.arrWx, data.includeArrMetar, data.includeArrTaf));
  lines.push(...buildWeatherSectionLines('ALTN WX', data.altnWx, data.includeAltnMetar, data.includeAltnTaf));

  if (data.includeWindsAloft && hasAnyWindsLine(data.windsAloft)) {
    lines.push('WINDS ALOFT');

    const depLine = buildWindsLine('DEP', data.windsAloft?.dep);
    const arrLine = buildWindsLine('ARR', data.windsAloft?.arr);

    if (depLine) lines.push(depLine);
    if (arrLine) lines.push(arrLine);

    lines.push('');
  }

  lines.push(`RMK ${generateAutoRemark(data)}`);
  lines.push('');
  lines.push('END OF REPORT');

  return lines.join('\n');
}

function getSingleSourceStatus(text, fallback) {
  if (!text || text === 'NOT AVAILABLE') {
    return { mode: 'NO DATA', rank: 3 };
  }

  if (fallback) {
    return { mode: 'FALLBACK', rank: 2 };
  }

  return { mode: 'LIVE', rank: 1 };
}

function combineStatuses(statuses) {
  if (!statuses.length) {
    return { mode: 'NO DATA', label: 'NO DATA' };
  }

  let worst = statuses[0];
  for (const status of statuses) {
    if (status.rank > worst.rank) worst = status;
  }

  return {
    mode: worst.mode,
    label: worst.mode
  };
}

function summarizeAirportStatus(wx, icao, includeMetar, includeTaf) {
  if (!icao) {
    return {
      airport: '',
      mode: 'EMPTY',
      label: ''
    };
  }

  if (!wx) {
    return {
      airport: icao,
      mode: 'NO DATA',
      label: 'NO DATA'
    };
  }

  const statuses = [];

  if (includeMetar) {
    statuses.push(getSingleSourceStatus(wx.metar, wx.metarFallback));
  }

  if (includeTaf) {
    statuses.push(getSingleSourceStatus(wx.taf, wx.tafFallback));
  }

  if (!statuses.length) {
    if (wx.metar && wx.metar !== 'NOT AVAILABLE') {
      return {
        airport: icao,
        mode: wx.metarFallback ? 'FALLBACK' : 'LIVE',
        label: wx.metarFallback ? 'FALLBACK' : 'LIVE'
      };
    }

    return {
      airport: icao,
      mode: 'NO DATA',
      label: 'NO DATA'
    };
  }

  const combined = combineStatuses(statuses);

  return {
    airport: icao,
    mode: combined.mode,
    label: combined.label
  };
}

async function getSuggestedAlternate(arrIcao, currentAltnIcao = '') {
  const arr = normalizeIcao(arrIcao);
  const currentAltn = normalizeIcao(currentAltnIcao);

  if (!arr) return null;

  const arrWx = await getAirportWeather(arr);
  if (!arrivalNeedsAlternate(arrWx)) return null;

  const candidates = await getNearbyAirports(arr, SUGGESTED_ALTN_LIMIT);

  for (const candidate of candidates) {
    if (candidate.icao === currentAltn) continue;

    const wx = await getAirportWeather(candidate.icao);
    if (!wx || !wx.metar || wx.metar === 'NOT AVAILABLE') continue;
    if (arrivalNeedsAlternate(wx)) continue;

    return {
      airport: candidate.icao,
      distanceNm: candidate.distanceNm,
      weatherMode: wx.metarFallback ? 'FALLBACK' : 'LIVE'
    };
  }

  return null;
}function buildPerformanceOutputSection(
  kind,
  airport,
  wx,
  runwayIdent,
  runwayHeading,
  weightLbs,
  runwayCondition,
  runwayLengthFt,
  runwaySlopePct,
  runwayElevationFt,
  runwayWidthFt,
  runwaySurface
) {
  const metar = wx?.metar && wx.metar !== 'NOT AVAILABLE' ? String(wx.metar) : '';
  const wind = parseWindDetailed(metar);
  const oatC = parseTemperatureC(metar);
  const qnhHpa = parseQnhHpa(metar);
  const qnhIn = hpaToInHg(qnhHpa);
  const elevationFt = Number.isFinite(runwayElevationFt)
    ? Math.round(runwayElevationFt)
    : (Number.isFinite(airport?.elevationFt) ? Math.round(airport.elevationFt) : null);
  const densityAltitudeFt = computeDensityAltitudeFt(elevationFt, oatC, qnhHpa);

  const components =
    wind && !wind.variable && Number.isFinite(wind.direction) && Number.isFinite(wind.speed) && Number.isFinite(runwayHeading)
      ? computeWindComponents(wind.direction, wind.speed, runwayHeading)
      : { headwindKt: null, crosswindKt: null };

  if (kind === 'takeoff') {
    return {
      runwayAndWeather: {
        apt: {
          icao: airport?.icao || '',
          name: airport?.name || ''
        },
        wind: wind
          ? {
              raw: wind.raw,
              direction: wind.direction,
              speedKt: wind.speed,
              gustKt: wind.gust,
              variable: wind.variable
            }
          : null,
        rwy: runwayIdent || '',
        hwXw: {
          headwindKt: components.headwindKt,
          crosswindKt: components.crosswindKt
        },
        toraFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
        oatC,
        hdgDeg: runwayHeading,
        qnhIn,
        qnhHpa,
        elevFt: elevationFt,
        rwyCond: runwayCondition || '',
        densityAltFt: densityAltitudeFt,
        rwySlopePct: Number.isFinite(runwaySlopePct) ? runwaySlopePct : null,
        widthFt: Number.isFinite(runwayWidthFt) ? runwayWidthFt : null,
        surface: runwaySurface || ''
      },
      inputs: {
        weightLbs: Number.isFinite(weightLbs) ? weightLbs : AIRCRAFT_PROFILE.maxTakeoffWeightLbs,
        flaps: AIRCRAFT_PROFILE.takeoff.defaultFlaps,
        powerSetting: AIRCRAFT_PROFILE.takeoff.powerSetting,
        ac: AIRCRAFT_PROFILE.takeoff.ac
      },
      outputs: {
        vrKias: null,
        vyKias: null,
        vxKias: null,
        accelCheckFt: null,
        rwyLimitFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
        limitCode: AIRCRAFT_PROFILE.takeoff.limitCode,
        takeoffDistanceRequiredFt: null,
        distance50FtFt: null,
        rotatePitch: AIRCRAFT_PROFILE.takeoff.rotatePitch,
        xwindLimitKt: AIRCRAFT_PROFILE.takeoff.xwindLimitKt
      },
      messages: ['NONE']
    };
  }

  return {
    airportAndWeather: {
      apt: {
        icao: airport?.icao || '',
        name: airport?.name || ''
      },
      wind: wind
        ? {
            raw: wind.raw,
            direction: wind.direction,
            speedKt: wind.speed,
            gustKt: wind.gust,
            variable: wind.variable
          }
        : null,
      rwy: runwayIdent || '',
      hwXw: {
        headwindKt: components.headwindKt,
        crosswindKt: components.crosswindKt
      },
      ldaFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
      oatC,
      hdgDeg: runwayHeading,
      qnhIn,
      qnhHpa,
      elevFt: elevationFt,
      rwyCond: runwayCondition || '',
      densityAltFt: densityAltitudeFt,
      rwySlopePct: Number.isFinite(runwaySlopePct) ? runwaySlopePct : null,
      widthFt: Number.isFinite(runwayWidthFt) ? runwayWidthFt : null,
      surface: runwaySurface || ''
    },
    inputs: {
      landingWeightLbs: Number.isFinite(weightLbs) ? weightLbs : AIRCRAFT_PROFILE.maxLandingWeightLbs,
      flaps: AIRCRAFT_PROFILE.landing.defaultFlaps,
      approachSpeedKias: null,
      ac: AIRCRAFT_PROFILE.landing.ac
    },
    outputs: {
      vrefKias: null,
      finalApproachSpeedKias: null,
      landingDistanceRequiredFt: null,
      distance50FtFt: null,
      rwyLimitFt: Number.isFinite(runwayLengthFt) ? runwayLengthFt : null,
      limitCode: AIRCRAFT_PROFILE.landing.limitCode,
      braking: AIRCRAFT_PROFILE.landing.braking
    },
    goAround: {
      goAroundPower: AIRCRAFT_PROFILE.landing.goAroundPower,
      flapsUpSpeedKias: null
    }
  };
}

async function buildPerformanceData(query) {
  const dep = normalizeIcao(query.dep);
  const arr = normalizeIcao(query.arr);

  const takeoffRunwayIdent = normalizeRunwayEndIdent(query.takeoffRunway || query.depRunway || '');
  const landingRunwayIdent = normalizeRunwayEndIdent(query.landingRunway || query.arrRunway || '');

  const [depAirport, arrAirport, depWx, arrWx, takeoffRunwayData, landingRunwayData] = await Promise.all([
    dep ? findAirportByIcao(dep) : null,
    arr ? findAirportByIcao(arr) : null,
    dep ? getAirportWeather(dep) : null,
    arr ? getAirportWeather(arr) : null,
    dep && takeoffRunwayIdent ? findRunwayDirection(dep, takeoffRunwayIdent) : null,
    arr && landingRunwayIdent ? findRunwayDirection(arr, landingRunwayIdent) : null
  ]);

  const takeoffRunwayHeading =
    normalizeHeading(query.takeoffRunwayHeading ?? query.depRunwayHeading) ??
    takeoffRunwayData?.heading ??
    null;

  const landingRunwayHeading =
    normalizeHeading(query.landingRunwayHeading ?? query.arrRunwayHeading) ??
    landingRunwayData?.heading ??
    null;

  const takeoffWeightLbs = normalizeWeight(query.takeoffWeightLbs ?? query.weightLbs);
  const landingWeightLbs = normalizeWeight(query.landingWeightLbs ?? query.weightLbs);

  const takeoffRunwayCondition =
    String(query.takeoffRunwayCondition || '').trim().toUpperCase() ||
    takeoffRunwayData?.surface ||
    '';

  const landingRunwayCondition =
    String(query.landingRunwayCondition || '').trim().toUpperCase() ||
    landingRunwayData?.surface ||
    '';

  const takeoffRunwayLengthFt =
    normalizeWeight(query.takeoffRunwayLengthFt ?? query.toraFt) ??
    takeoffRunwayData?.toraFt ??
    null;

  const landingRunwayLengthFt =
    normalizeWeight(query.landingRunwayLengthFt ?? query.ldaFt) ??
    landingRunwayData?.ldaFt ??
    null;

  const landingRunwaySlopePct =
    normalizeDecimal(query.landingRunwaySlopePct) ??
    landingRunwayData?.slopePct ??
    null;

  const takeoffRunwaySlopePct =
    normalizeDecimal(query.takeoffRunwaySlopePct) ??
    takeoffRunwayData?.slopePct ??
    null;

  const takeoffRunwayElevationFt =
    normalizeWeight(query.takeoffRunwayElevationFt ?? query.depRunwayElevationFt) ??
    takeoffRunwayData?.elevationFt ??
    null;

  const landingRunwayElevationFt =
    normalizeWeight(query.landingRunwayElevationFt ?? query.arrRunwayElevationFt) ??
    landingRunwayData?.elevationFt ??
    null;

  const takeoffRunwayWidthFt =
    normalizeWeight(query.takeoffRunwayWidthFt) ??
    takeoffRunwayData?.widthFt ??
    null;

  const landingRunwayWidthFt =
    normalizeWeight(query.landingRunwayWidthFt) ??
    landingRunwayData?.widthFt ??
    null;

  return {
    aircraft: AIRCRAFT_PROFILE,
    takeoff: buildPerformanceOutputSection(
      'takeoff',
      depAirport,
      depWx,
      takeoffRunwayIdent,
      takeoffRunwayHeading,
      takeoffWeightLbs,
      takeoffRunwayCondition,
      takeoffRunwayLengthFt,
      takeoffRunwaySlopePct,
      takeoffRunwayElevationFt,
      takeoffRunwayWidthFt,
      takeoffRunwayData?.surface || takeoffRunwayCondition
    ),
    landing: buildPerformanceOutputSection(
      'landing',
      arrAirport,
      arrWx,
      landingRunwayIdent,
      landingRunwayHeading,
      landingWeightLbs,
      landingRunwayCondition,
      landingRunwayLengthFt,
      landingRunwaySlopePct,
      landingRunwayElevationFt,
      landingRunwayWidthFt,
      landingRunwayData?.surface || landingRunwayCondition
    )
  };
}

async function dispatchToPdfBuffer(data) {
  const pdfDoc = await PDFDocument.create();
  const regularFont = await pdfDoc.embedFont(StandardFonts.Courier);
  const boldFont = await pdfDoc.embedFont(StandardFonts.CourierBold);

  const pageWidth = 595.28;
  const pageHeight = 841.89;
  const page = pdfDoc.addPage([pageWidth, pageHeight]);

  page.drawRectangle({
    x: 0,
    y: 0,
    width: pageWidth,
    height: pageHeight,
    color: rgb(1, 1, 1)
  });

  const black = rgb(0, 0, 0);
  const blockWidth = 311.81;
  const left = (pageWidth - blockWidth) / 2;
  const indent = 12;
  let y = pageHeight - 36;

  const drawLine = (text, size = 10, bold = false, x = left) => {
    y -= size;
    page.drawText(text, {
      x,
      y,
      size,
      font: bold ? boldFont : regularFont,
      color: black
    });
    y -= 3.5;
  };

  const drawWrappedText = (text, size = 9.2, x = left + indent) => {
    const wrapped = wrapText(text, 50);
    for (const line of wrapped) {
      y -= size;
      page.drawText(line, {
        x,
        y,
        size,
        font: regularFont,
        color: black
      });
      y -= 3;
    }
  };

  const drawWeatherSection = (title, wx, wantMetar, wantTaf) => {
    if (!wx || (!wantMetar && !wantTaf)) return;

    drawLine(`${title} (${wx.airport})`, 10.5, true);

    if (wantMetar) {
      if (wx.metarFallback && wx.metarSource) {
        drawLine('METAR MODE FALLBACK', 9.2, true);
        drawLine(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`, 9.2, true);
      } else if (wx.metar && wx.metar !== 'NOT AVAILABLE') {
        drawLine('METAR MODE LIVE', 9.2, true);
      } else {
        drawLine('METAR MODE NO DATA', 9.2, true);
      }

      drawWrappedText(wx.metar || 'NOT AVAILABLE');
      y -= 4;
    }

    if (wantTaf) {
      if (wx.tafFallback && wx.tafSource) {
        drawLine('TAF MODE FALLBACK', 9.2, true);
        drawLine(`TAF SRC ${wx.tafSource}/${wx.tafDistanceNm}NM`, 9.2, true);
      } else if (wx.taf && wx.taf !== 'NOT AVAILABLE') {
        drawLine('TAF MODE LIVE', 9.2, true);
      } else {
        drawLine('TAF MODE NO DATA', 9.2, true);
      }

      drawWrappedText(wx.taf || 'NOT AVAILABLE');
      y -= 4;
    }
  };

  const headerLine1 = [
    'OPS NATGLOBE AVIATION',
    `DATE ${data.date}`,
    `UTC ${data.timeUtc.replace(' UTC', '')}`
  ].join('   ');

  const headerLine2 = [
    data.flight ? `FLT ${data.flight}` : null,
    data.dep ? `ORIG ${data.dep}` : null,
    data.arr ? `DEST ${data.arr}` : null,
    data.route ? `ROUTE ${data.route}` : null
  ].filter(Boolean).join('   ');

  drawLine('ACARS WEATHER REPORT', 12, true);
  drawLine('--------------------', 11, true);
  drawLine(headerLine1, 9.5, false);
  drawLine(headerLine2, 9.5, false);
  y -= 4;

  drawWeatherSection('DEP WX', data.depWx, data.includeDepMetar, data.includeDepTaf);
  drawWeatherSection('ARR WX', data.arrWx, data.includeArrMetar, data.includeArrTaf);
  drawWeatherSection('ALTN WX', data.altnWx, data.includeAltnMetar, data.includeAltnTaf);

  if (data.includeWindsAloft && hasAnyWindsLine(data.windsAloft)) {
    drawLine('WINDS ALOFT', 10.2, true);

    const depLine = buildWindsLine('DEP', data.windsAloft?.dep);
    const arrLine = buildWindsLine('ARR', data.windsAloft?.arr);

    if (depLine) drawWrappedText(depLine, 9.2, left + indent);
    if (arrLine) drawWrappedText(arrLine, 9.2, left + indent);

    y -= 4;
  }

  drawLine(`RMK ${generateAutoRemark(data)}`, 9.5, true);
  y -= 2;

  drawLine('END OF REPORT', 10.5, true);

  return Buffer.from(await pdfDoc.save());
}

app.get('/booking', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'booking.html'));
});

app.get('/bookings', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'booking.html'));
});

app.get('/NGA-PRIVATE-FLIGHT-BOOKINGS', (req, res) => {
  res.redirect('/bookings');
});

app.get('/booking-confirmation', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'booking-confirmation.html'));
});

app.get('/manage-booking/:reference/:token', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'manage-booking.html'));
});

app.get('/booking-start', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'booking-login.html'));
});

app.get('/booking-portal', (req, res) => {
  res.redirect('/booking-start');
});

app.get('/booking-login', (req, res) => {
  res.redirect('/booking-start');
});

app.get('/portal', (req, res) => {
  res.redirect('/booking-start');
});

app.get('/booking-ops', (req, res) => {
  const fileName = hasPilotAccess(req) ? 'booking-ops.html' : 'pilot-login.html';
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', fileName));
});

app.get('/booking-ops/requests/:id/reimbursement-statement', requirePilotAccess, (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'reimbursement-statement.html'));
});

app.get('/booking-ops/requests/:id/operational-flight-plan', requirePilotAccess, (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'operational-flight-plan.html'));
});

function sendBoardingPassPage(req, res) {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'boarding-pass.html'));
}

app.get('/boarding-pass/:token', (req, res) => {
  sendBoardingPassPage(req, res);
});

app.get('/pass/:reference/passenger-information-pass/:token', (req, res) => {
  sendBoardingPassPage(req, res);
});

app.get('/private-flight-information-pass/:reference/:token', (req, res) => {
  sendBoardingPassPage(req, res);
});

app.get('/private-flight-agreement/:reference/:token', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'private-flight-agreement.html'));
});

function findPublicReimbursementStatement(reference, token) {
  const request = bookingRequests.find(item => (
    item.id === String(reference || '').trim().toUpperCase()
    && item.reimbursementStatementToken === String(token || '').trim()
  ));
  if (!request || !isBoardingPassReady(request) || !request.reimbursementStatement) return null;
  return request;
}

app.get('/private-flight-reimbursement-statement/:reference/:token', async (req, res) => {
  await bookingStoreReady;
  const request = findPublicReimbursementStatement(req.params.reference, req.params.token);
  if (!request) return res.status(404).send('Reimbursement Statement not available.');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'reimbursement-statement.html'));
});

app.get('/private-flight-time-proposal/:reference/:token/:action?', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'time-proposal-response.html'));
});

function findTimeProposalRequest(reference, token) {
  const request = bookingRequests.find(item => (
    item.id === String(reference || '').trim().toUpperCase()
    && item.timeProposal?.token === String(token || '').trim()
  ));
  if (!request?.timeProposal) return null;
  return request;
}

app.get('/api/private-flight-time-proposals/:reference/:token', async (req, res) => {
  await bookingStoreReady;
  const request = findTimeProposalRequest(req.params.reference, req.params.token);
  if (!request) return res.status(404).json({ error: 'TIME PROPOSAL NOT AVAILABLE' });
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({
    proposal: {
      reference: request.id,
      route: `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`,
      currentDate: request.timeProposal.originalDate || request.requestDate || '',
      currentTime: request.timeProposal.originalTime || request.requestTime || '',
      proposedDate: request.timeProposal.date || '',
      proposedTime: request.timeProposal.time || '',
      proposedLabel: proposedTimeLabel(request.timeProposal),
      note: request.timeProposal.note || '',
      responseStatus: request.timeProposal.responseStatus || 'PENDING',
      responseMessage: request.timeProposal.responseMessage || '',
      respondedAt: request.timeProposal.respondedAt || ''
    }
  });
});

app.post('/api/private-flight-time-proposals/:reference/:token/respond', async (req, res) => {
  await bookingStoreReady;
  const request = findTimeProposalRequest(req.params.reference, req.params.token);
  if (!request) return res.status(404).json({ error: 'TIME PROPOSAL NOT AVAILABLE' });
  const decision = String(req.body?.decision || '').trim().toUpperCase();
  if (!['ACCEPTED', 'DENIED'].includes(decision)) return res.status(400).json({ error: 'SELECT ACCEPT OR DENY' });
  const message = normalizeBookingText(req.body?.message, 500);
  request.timeProposal.responseStatus = decision;
  request.timeProposal.responseMessage = message;
  request.timeProposal.respondedAt = new Date().toISOString();
  if (decision === 'ACCEPTED') {
    request.requestDate = request.timeProposal.date;
    request.requestTime = request.timeProposal.time;
    await addBookingTimelineEvent(request.id, 'TIME PROPOSAL ACCEPTED', message || proposedTimeLabel(request.timeProposal));
  } else {
    await addBookingTimelineEvent(request.id, 'TIME PROPOSAL DENIED', message || 'Passenger declined the proposed departure time.');
  }
  await persistBookingRequest(request);
  res.json({
    ok: true,
    reference: request.id,
    status: request.timeProposal.responseStatus,
    proposedLabel: proposedTimeLabel(request.timeProposal)
  });
});

app.get('/pass-check/:token', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'pass-verification.html'));
});

function agreementSignatureMatches(passenger, signatureName) {
  const normalize = value => normalizeBookingText(value, 80).replace(/\s+/g, ' ').trim().toUpperCase();
  return Boolean(signatureName) && normalize(passenger.name) === normalize(signatureName);
}

function findAgreementByToken(token) {
  const request = bookingRequests.find(item => getRequestPassengers(item).some(passenger => passenger.agreementToken === token));
  if (!request || !isBoardingPassReady(request)) return { request: null, passenger: null };
  const passenger = getRequestPassengers(request).find(item => item.agreementToken === token) || null;
  return { request, passenger };
}

app.get('/api/private-flight-agreements/:token', async (req, res) => {
  await bookingStoreReady;
  const { request, passenger } = findAgreementByToken(String(req.params.token || '').trim());
  if (!request || !passenger) return res.status(404).json({ error: 'AGREEMENT NOT AVAILABLE' });
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({
    agreement: {
      reference: request.id,
      status: passenger.agreementSignedAt ? 'SIGNED' : 'AWAITING SIGNATURE',
      signedAt: passenger.agreementSignedAt || '',
      passenger: {
        title: passenger.title || '',
        name: passenger.name || 'Passenger',
        dob: passenger.dob || '',
        passportCountry: passenger.passportCountry || '',
        passport: maskedPassport(passenger)
      },
      flight: {
        route: `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`,
        date: emailDate(request.requestDate),
        boardingTime: `${boardingPassBoardingTime(request.requestTime)} Local Time`,
        departureTime: `${formatBoardingPassTime(request.requestTime)} Local Time`,
        flightTime: emailFlightTime(request),
        aircraft: PUBLIC_AIRCRAFT_TYPE,
        baggage: boardingPassBaggage(request)
      }
    }
  });
});

function publicReimbursementStatementView(request) {
  return {
    id: request.id,
    dep: request.dep,
    arr: request.arr,
    depName: request.depName,
    arrName: request.arrName,
    seats: request.seats,
    extras: request.extras,
    estimatedFlightMinutes: request.estimatedFlightMinutes,
    crew: request.crew,
    passengers: getRequestPassengers(request).map(passenger => ({
      number: passenger.number,
      name: passenger.name,
      email: passenger.email,
      phone: passenger.phone
    })),
    reimbursementStatement: request.reimbursementStatement,
    siirtoPhone: request.reimbursementStatement?.siirtoPhone || String(process.env.BOOKING_SIIRTO_PHONE || '').trim(),
    paymentUrl: bookingPaymentUrl(request)
  };
}

app.get('/api/private-flight-reimbursement-statement/:reference/:token', async (req, res) => {
  await bookingStoreReady;
  const request = findPublicReimbursementStatement(req.params.reference, req.params.token);
  if (!request) return res.status(404).json({ error: 'REIMBURSEMENT STATEMENT NOT AVAILABLE' });
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({ request: publicReimbursementStatementView(request) });
});

app.get('/api/private-flight-reimbursement-statement/:reference/:token/pdf', async (req, res) => {
  await bookingStoreReady;
  const request = findPublicReimbursementStatement(req.params.reference, req.params.token);
  if (!request) return res.status(404).json({ error: 'REIMBURSEMENT STATEMENT NOT AVAILABLE' });
  const pdf = await createReimbursementStatementPdf(request);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="${request.id}-REIMBURSEMENT-STATEMENT.pdf"`);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.send(pdf);
});

app.post('/api/private-flight-agreements/:token/sign', async (req, res) => {
  await bookingStoreReady;
  const { request, passenger } = findAgreementByToken(String(req.params.token || '').trim());
  if (!request || !passenger) return res.status(404).json({ error: 'AGREEMENT NOT AVAILABLE' });
  if (passenger.agreementSignedAt) return res.json({ ok: true, signedAt: passenger.agreementSignedAt, alreadySigned: true });

  const signatureName = normalizeBookingText(req.body?.signatureName, 80);
  const accepted = req.body?.accepted === true || req.body?.accepted === 'true';
  if (!accepted) return res.status(400).json({ error: 'AGREEMENT ACCEPTANCE REQUIRED' });
  if (!agreementSignatureMatches(passenger, signatureName)) {
    return res.status(400).json({ error: 'TYPE YOUR FULL NAME EXACTLY AS SHOWN IN THE AGREEMENT' });
  }

  passenger.agreementAccepted = true;
  passenger.agreementSignatureName = signatureName;
  passenger.agreementSignedAt = new Date().toISOString();
  passenger.agreementVersion = 'PRIVATE-FLIGHT-AGREEMENT-2026-06-24';
  await persistBookingRequest(request);
  await addBookingTimelineEvent(request.id, 'PASSENGER AGREEMENT SIGNED', `${passenger.name || `PAX ${passenger.number || 1}`} / PAX ${passenger.number || 1}`);
  res.json({ ok: true, signedAt: passenger.agreementSignedAt });
});

app.post('/api/pilot-login', (req, res) => {
  const code = String(req.body?.code || '').trim();
  const profile = pilotAccessProfiles().find(item => item.code === code);
  if (!profile) {
    return res.status(401).json({ error: 'INVALID PILOT CODE' });
  }

  res.setHeader('Set-Cookie', pilotCookieOptions(profile));
  res.json({ ok: true, role: profile.role, name: profile.name });
});

app.get('/api/pilot-session', requirePilotAccess, (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({ session: req.pilotSession });
});

app.get('/api/pilot-passkeys', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const passkeys = passkeysForProfile(req.pilotSession).map(passkeyPublicView);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({ passkeys });
});

app.post('/api/pilot-passkeys/registration/options', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  try {
    const profile = {
      role: passkeyProfileKey(req.pilotSession),
      name: normalizeBookingText(req.pilotSession.name, 60).toUpperCase()
    };
    const { rpID, expectedOrigin } = webAuthnConfiguration();
    const options = await generateRegistrationOptions({
      rpName: WEBAUTHN_RP_NAME,
      rpID,
      userID: new Uint8Array(Buffer.from(`nga-private-aviation/${profile.role}`, 'utf8')),
      userName: `nga-${profile.role.toLowerCase()}`,
      userDisplayName: `${profile.name} / ${profile.role}`,
      timeout: 60000,
      attestationType: 'none',
      excludeCredentials: passkeysForProfile(profile).map(passkey => ({
        id: passkey.credentialId,
        transports: passkey.transports || []
      })),
      authenticatorSelection: {
        residentKey: 'required',
        userVerification: 'required'
      },
      preferredAuthenticatorType: 'localDevice'
    });
    const transactionId = rememberPasskeyChallenge(passkeyRegistrationChallenges, {
      challenge: options.challenge,
      profile,
      rpID,
      expectedOrigin
    });
    res.json({ options, transactionId });
  } catch (err) {
    console.error('PASSKEY REGISTRATION OPTIONS:', err.message);
    res.status(500).json({ error: 'UNABLE TO START DEVICE PASSKEY SETUP' });
  }
});

app.post('/api/pilot-passkeys/registration/verify', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const challenge = consumePasskeyChallenge(passkeyRegistrationChallenges, req.body?.transactionId);
  if (!challenge) return res.status(400).json({ error: 'PASSKEY SETUP EXPIRED. TRY AGAIN.' });
  if (passkeyProfileKey(req.pilotSession) !== challenge.profile.role) {
    return res.status(403).json({ error: 'PILOT SESSION DOES NOT MATCH PASSKEY SETUP' });
  }
  try {
    const verification = await verifyRegistrationResponse({
      response: req.body?.credential,
      expectedChallenge: challenge.challenge,
      expectedOrigin: challenge.expectedOrigin,
      expectedRPID: challenge.rpID,
      requireUserVerification: true
    });
    if (!verification.verified || !verification.registrationInfo) {
      return res.status(400).json({ error: 'DEVICE PASSKEY COULD NOT BE VERIFIED' });
    }
    const credential = verification.registrationInfo.credential;
    const passkey = {
      id: `PK-${randomUUID().slice(0, 12).toUpperCase()}`,
      profileRole: challenge.profile.role,
      profileName: challenge.profile.name,
      credentialId: credential.id,
      publicKey: Buffer.from(credential.publicKey).toString('base64url'),
      counter: Number(credential.counter || 0),
      transports: Array.isArray(credential.transports) ? credential.transports : [],
      createdAt: new Date().toISOString(),
      lastUsedAt: null
    };
    await persistPilotPasskey(passkey);
    res.json({ ok: true, passkey: passkeyPublicView(passkey) });
  } catch (err) {
    console.error('PASSKEY REGISTRATION VERIFY:', err.message);
    res.status(400).json({ error: 'DEVICE PASSKEY SETUP FAILED' });
  }
});

app.post('/api/pilot-passkeys/authentication/options', async (req, res) => {
  await bookingStoreReady;
  if (!pilotPasskeys.size) {
    return res.status(409).json({ error: 'NO DEVICE PASSKEY IS SET UP. USE THE PILOT ACCESS CODE FIRST.' });
  }
  try {
    const { rpID, expectedOrigin } = webAuthnConfiguration();
    const options = await generateAuthenticationOptions({
      rpID,
      timeout: 60000,
      userVerification: 'required'
    });
    const transactionId = rememberPasskeyChallenge(passkeyAuthenticationChallenges, {
      challenge: options.challenge,
      rpID,
      expectedOrigin
    });
    res.json({ options, transactionId });
  } catch (err) {
    console.error('PASSKEY AUTHENTICATION OPTIONS:', err.message);
    res.status(500).json({ error: 'UNABLE TO START DEVICE PASSKEY LOGIN' });
  }
});

app.post('/api/pilot-passkeys/authentication/verify', async (req, res) => {
  await bookingStoreReady;
  const challenge = consumePasskeyChallenge(passkeyAuthenticationChallenges, req.body?.transactionId);
  if (!challenge) return res.status(400).json({ error: 'PASSKEY LOGIN EXPIRED. TRY AGAIN.' });
  const credentialId = String(req.body?.credential?.id || '').trim();
  const storedPasskey = pilotPasskeys.get(credentialId);
  if (!storedPasskey) return res.status(401).json({ error: 'THIS DEVICE PASSKEY IS NOT REGISTERED' });
  const profile = pilotAccessProfiles().find(item => item.role === storedPasskey.profileRole);
  if (!profile) return res.status(403).json({ error: 'THE PILOT ROLE FOR THIS PASSKEY IS NO LONGER ACTIVE' });
  try {
    const verification = await verifyAuthenticationResponse({
      response: req.body?.credential,
      expectedChallenge: challenge.challenge,
      expectedOrigin: challenge.expectedOrigin,
      expectedRPID: challenge.rpID,
      credential: {
        id: storedPasskey.credentialId,
        publicKey: Buffer.from(storedPasskey.publicKey, 'base64url'),
        counter: Number(storedPasskey.counter || 0),
        transports: storedPasskey.transports || []
      },
      requireUserVerification: true
    });
    if (!verification.verified) return res.status(401).json({ error: 'DEVICE PASSKEY COULD NOT BE VERIFIED' });
    storedPasskey.counter = Number(verification.authenticationInfo.newCounter || 0);
    storedPasskey.lastUsedAt = new Date().toISOString();
    storedPasskey.profileName = profile.name;
    await persistPilotPasskey(storedPasskey);
    res.setHeader('Set-Cookie', pilotCookieOptions(profile));
    res.json({ ok: true, role: profile.role, name: profile.name });
  } catch (err) {
    console.error('PASSKEY AUTHENTICATION VERIFY:', err.message);
    res.status(401).json({ error: 'DEVICE PASSKEY LOGIN FAILED' });
  }
});

app.delete('/api/pilot-passkeys/:id', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const passkey = [...pilotPasskeys.values()].find(item => item.id === req.params.id);
  if (!passkey || passkey.profileRole !== passkeyProfileKey(req.pilotSession)) {
    return res.status(404).json({ error: 'DEVICE PASSKEY NOT FOUND' });
  }
  await removePilotPasskey(passkey.credentialId);
  res.json({ ok: true });
});

app.post('/api/pilot-logout', (req, res) => {
  res.setHeader('Set-Cookie', `${PILOT_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
  res.json({ ok: true });
});

app.get('/api/booking-airports', async (req, res) => {
  try {
    const airports = await getBookingAirportCatalog();
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({ airports });
  } catch (err) {
    console.error(err);
    res.status(500).json({ airports: bookingAirports });
  }
});

app.get('/api/cost-share-flights', async (req, res) => {
  await bookingStoreReady;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({
    flights: costShareFlights.map(publicFlightView),
    requests: bookingRequests.slice(-8).reverse()
  });
});

app.get('/api/booking-availability', async (req, res) => {
  await bookingStoreReady;
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({
    dates: [...bookingAvailability.entries()].map(([date, item]) => ({ date, ...item }))
  });
});

function bookingAccessByEmail(referenceInput, emailInput) {
  const reference = normalizeBookingText(referenceInput, 40).toUpperCase();
  const email = normalizeBookingEmail(emailInput);
  if (!reference || !email || !email.includes('@')) return { error: 'REFERENCE AND PASSENGER EMAIL REQUIRED', status: 400 };
  const request = bookingRequests.find(item => item.id === reference || item.flightId === reference);
  if (!request) return { error: 'BOOKING REQUEST NOT FOUND', status: 404 };
  const passengers = getRequestPassengers(request);
  const matchingPassenger = passengers.find(passenger => normalizeBookingEmail(passenger.email) === email) || null;
  const extraEmails = normalizeAdditionalEmailContacts(request.additionalEmailContacts).map(item => normalizeBookingEmail(item));
  const payerEmail = normalizeBookingEmail(request.reimbursementStatement?.payerEmail);
  const requestEmail = normalizeBookingEmail(request.email);
  const allowed = Boolean(matchingPassenger || requestEmail === email || extraEmails.includes(email) || (payerEmail && payerEmail === email));
  if (!allowed) return { error: 'BOOKING REQUEST NOT FOUND FOR THIS EMAIL', status: 404 };
  return { reference, email, request, passengers, matchingPassenger, requestEmail };
}

function managementAccessByToken(referenceInput, tokenInput) {
  const reference = normalizeBookingText(referenceInput, 40).toUpperCase();
  const token = normalizeBookingText(tokenInput, 80);
  const request = bookingRequests.find(item => item.id === reference || item.flightId === reference);
  if (!request || !token) return null;
  const passengers = getRequestPassengers(request);
  const passenger = passengers.find(item => item.managementToken === token) || null;
  return passenger ? { request, passengers, passenger } : null;
}

const MANAGE_BAG_LIMITS = {
  'CARRY-ON SUITCASE': 15,
  'SOFT DUFFLE BAG': 20,
  'WEEKEND HOLDALL': 15,
  'SMALL BACKPACK': 8,
  'GARMENT BAG': 10,
  'OTHER BAG - PILOT REVIEW': 20
};
const MANAGE_SEATS = new Set(['REAR LEFT WINDOW', 'REAR RIGHT WINDOW', 'NO PREFERENCE']);
const MANAGE_EXTRAS = ['WINE', 'CHAMPAGNE', 'BEER', 'SOFT DRINKS / WATER', 'SNACKS', 'AIRPORT TRANSFERS'];

function selectedManageExtras(request) {
  const source = String(request.extras || '').toUpperCase();
  return MANAGE_EXTRAS.filter(item => source.includes(item));
}

function bookingManagementView(request, passenger) {
  const isLeadPassenger = Number(passenger.number || 1) === 1 || normalizeBookingEmail(passenger.email) === normalizeBookingEmail(request.email);
  const pendingStage = ['REQUESTED', 'NEEDS INFO', 'WAITLIST'].includes(String(request.status || '').toUpperCase()) && request.pilotDecision !== 'APPROVED';
  const managementRequest = request.managementRequest || null;
  const isClosed = ['DECLINED', 'CANCELLED'].includes(String(request.status || '').toUpperCase());
  return {
    reference: request.id,
    statusLabel: request.status || 'REQUESTED',
    pilotDecision: request.pilotDecision || 'PENDING',
    paymentStatus: request.paymentStatus || 'UNPAID',
    route: `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`,
    date: emailDate(request.requestDate),
    departureTime: `${formatBoardingPassTime(request.requestTime)} Local Time`,
    passengerName: passenger.name || 'Passenger',
    phone: passenger.phone || '',
    carryOnBags: request.carryOnBags === 'YES' ? 'YES' : 'NO',
    bagType: request.bagType || 'CARRY-ON SUITCASE',
    baggageWeightKg: request.carryOnBags === 'YES' ? String(request.baggageWeightKg || '') : '',
    powerBanks: request.powerBanks === 'YES' ? 'YES' : 'NO',
    seatPreference: MANAGE_SEATS.has(request.seatPreference) ? request.seatPreference : 'NO PREFERENCE',
    extras: selectedManageExtras(request),
    extrasNotes: request.extrasNotes || '',
    canDirectEdit: Boolean(isLeadPassenger && pendingStage),
    isClosed,
    managementRequest: managementRequest ? {
      type: managementRequest.type,
      status: managementRequest.status,
      submittedAt: managementRequest.submittedAt,
      resolvedAt: managementRequest.resolvedAt || ''
    } : null
  };
}

function normalizeManagementPatch(body) {
  const phone = normalizeBookingText(body?.phone, 40);
  const carryOnBags = body?.carryOnBags === 'YES' ? 'YES' : 'NO';
  const bagType = Object.hasOwn(MANAGE_BAG_LIMITS, body?.bagType) ? body.bagType : 'CARRY-ON SUITCASE';
  const baggageWeight = Number(String(body?.baggageWeightKg || '').replace(',', '.'));
  if (carryOnBags === 'YES' && (!Number.isFinite(baggageWeight) || baggageWeight <= 0 || baggageWeight > MANAGE_BAG_LIMITS[bagType])) {
    return { error: `BAGGAGE WEIGHT MUST BE BETWEEN 1 AND ${MANAGE_BAG_LIMITS[bagType]} KG FOR THIS BAG TYPE` };
  }
  const seatPreference = MANAGE_SEATS.has(body?.seatPreference) ? body.seatPreference : 'NO PREFERENCE';
  const extrasInput = Array.isArray(body?.extras) ? body.extras : [];
  const extras = MANAGE_EXTRAS.filter(item => extrasInput.includes(item));
  return {
    patch: {
      phone,
      carryOnBags,
      bagType: carryOnBags === 'YES' ? bagType : '',
      baggageWeightKg: carryOnBags === 'YES' ? String(baggageWeight) : '',
      powerBanks: body?.powerBanks === 'YES' ? 'YES' : 'NO',
      seatPreference,
      extras,
      extrasNotes: normalizeBookingText(body?.extrasNotes, 260)
    }
  };
}

function applyManagementPatch(request, passenger, patch) {
  passenger.phone = patch.phone || passenger.phone || '';
  if (Number(passenger.number || 1) === 1) request.phone = passenger.phone;
  request.carryOnBags = patch.carryOnBags;
  request.bagType = patch.bagType;
  request.baggageWeightKg = patch.baggageWeightKg;
  request.powerBanks = patch.powerBanks;
  request.seatPreference = patch.seatPreference;
  request.extras = patch.extras.join(' / ');
  request.extrasNotes = patch.extrasNotes;
  request.lastPassengerUpdateAt = new Date().toISOString();
}

function managementPatchSummary(patch) {
  return [
    `PHONE ${patch.phone || 'UNCHANGED'}`,
    `BAGGAGE ${patch.carryOnBags === 'YES' ? `${patch.bagType} / ${patch.baggageWeightKg}KG` : 'NONE'}`,
    `POWER BANK ${patch.powerBanks}`,
    `SEAT ${patch.seatPreference}`,
    `EXTRAS ${patch.extras.length ? patch.extras.join(', ') : 'NONE'}`,
    patch.extrasNotes ? `NOTES ${patch.extrasNotes}` : ''
  ].filter(Boolean).join(' / ');
}

async function notifyPilotOfBookingManagement(request, label, detail) {
  const recipient = String(process.env.PILOT_NOTIFICATION_EMAIL || '').trim();
  if (!recipient) return false;
  return sendBookingEmail({
    to: recipient,
    subject: `${label} ${request.id}`,
    text: [
      `Reference: ${request.id}`,
      `Route: ${request.depName || request.dep} to ${request.arrName || request.arr}`,
      `Departure: ${request.requestDate} ${request.requestTime}`,
      detail,
      '',
      'Open Booking Operations to review the flight file.'
    ].join('\n')
  });
}

app.post('/api/booking-status', async (req, res) => {
  await bookingStoreReady;
  const access = bookingAccessByEmail(req.body?.reference, req.body?.email);
  if (access.error) return res.status(access.status).json({ error: access.error });
  const { request, passengers, matchingPassenger, requestEmail } = access;

  const signedCount = passengers.filter(passenger => passenger.agreementSignedAt).length;
  const agreementStatus = passengers.length
    ? `${signedCount}/${passengers.length} signed`
    : 'Pending';
  const matchedPassPassenger = matchingPassenger || passengers.find(passenger => normalizeBookingEmail(passenger.email) === requestEmail) || passengers[0];
  const flightPassUrl = isBoardingPassReady(request) && matchedPassPassenger?.boardingPassToken
    ? `${PUBLIC_SITE_URL}/private-flight-information-pass/${encodeURIComponent(request.id)}/${matchedPassPassenger.boardingPassToken}`
    : '';

  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({
    request: {
      reference: request.id,
      statusLabel: request.status || 'REQUESTED',
      route: `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`,
      date: emailDate(request.requestDate),
      departureTime: `${formatBoardingPassTime(request.requestTime)} Local Time`,
      pilotDecision: request.pilotDecision || 'PENDING',
      paymentStatus: request.paymentStatus || 'UNPAID',
      agreementStatus,
      passengerCount: passengers.length,
      flightTime: emailFlightTime(request),
      flightPassUrl,
      canManage: Boolean(matchingPassenger),
      managementRequestStatus: request.managementRequest?.status || ''
    }
  });
});

app.post('/api/booking-manage-link', async (req, res) => {
  await bookingStoreReady;
  const access = bookingAccessByEmail(req.body?.reference, req.body?.email);
  if (access.error) return res.status(access.status).json({ error: access.error });
  const { request, matchingPassenger } = access;
  if (!matchingPassenger) return res.status(403).json({ error: 'MANAGE BOOKING ACCESS IS AVAILABLE TO NAMED PASSENGERS ONLY' });
  const throttleKey = `${request.id}:${normalizeBookingEmail(matchingPassenger.email)}`;
  const lastSentAt = bookingManageLinkThrottle.get(throttleKey) || 0;
  if (Date.now() - lastSentAt < 60000) return res.status(429).json({ error: 'A MANAGE BOOKING LINK WAS JUST SENT. PLEASE CHECK YOUR EMAIL.' });
  bookingManageLinkThrottle.set(throttleKey, Date.now());
  if (!matchingPassenger.managementToken) matchingPassenger.managementToken = createBookingManagementToken();
  await persistBookingRequest(request);
  const url = `${PUBLIC_SITE_URL}/manage-booking/${encodeURIComponent(request.id)}/${matchingPassenger.managementToken}`;
  const html = privateFlightEmailHtml({
    status: 'MANAGE BOOKING',
    reference: request.id,
    greeting: emailPassengerName(matchingPassenger),
    intro: 'Use the private link below to review and manage the permitted parts of your flight request.',
    details: [
      ['Route', `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`],
      ['Date', emailDate(request.requestDate)],
      ['Departure', `${formatBoardingPassTime(request.requestTime)} Local Time`],
      ['Status', request.status || 'REQUESTED']
    ],
    sections: [{
      title: 'SECURE BOOKING ACCESS',
      html: `<p style="margin:8px 0 14px;line-height:1.6;color:#031c45">This link is personal to you. Do not forward it.</p><a href="${url}" style="display:inline-block;padding:12px 16px;background:#031c45;color:#ffffff;text-decoration:none;font-weight:700">OPEN MANAGE BOOKING</a>`
    }],
    closing: ['Best regards,', 'NGA Private Aviation Team', 'info.ngaprivateaviation@gmail.com']
  });
  try {
    const sent = await sendBookingEmail({
      to: matchingPassenger.email,
      subject: `Manage booking ${request.id}`,
      text: `Open your secure booking management page:\n${url}\n\nDo not forward this personal link.`,
      html
    });
    if (!sent) return res.status(503).json({ error: 'BOOKING EMAIL SERVICE IS NOT CONFIGURED' });
  } catch (error) {
    console.error('MANAGE BOOKING LINK:', error.message);
    return res.status(502).json({ error: 'UNABLE TO SEND MANAGE BOOKING LINK' });
  }
  await addBookingTimelineEvent(request.id, 'MANAGE LINK SENT', `Secure manage link sent to passenger ${matchingPassenger.number || 1}.`);
  res.json({ message: 'SECURE MANAGE BOOKING LINK SENT BY EMAIL' });
});

app.get('/api/manage-booking/:reference/:token', async (req, res) => {
  await bookingStoreReady;
  const access = managementAccessByToken(req.params.reference, req.params.token);
  if (!access) return res.status(404).json({ error: 'MANAGE BOOKING LINK IS NOT VALID' });
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({ booking: bookingManagementView(access.request, access.passenger) });
});

app.patch('/api/manage-booking/:reference/:token', async (req, res) => {
  await bookingStoreReady;
  const access = managementAccessByToken(req.params.reference, req.params.token);
  if (!access) return res.status(404).json({ error: 'MANAGE BOOKING LINK IS NOT VALID' });
  const { request, passenger } = access;
  const action = String(req.body?.action || 'UPDATE').trim().toUpperCase();
  const note = normalizeBookingText(req.body?.note, 300);
  const currentView = bookingManagementView(request, passenger);
  if (currentView.isClosed) return res.status(409).json({ error: 'THIS BOOKING IS CLOSED' });
  if (request.managementRequest?.status === 'PENDING') return res.status(409).json({ error: 'A PASSENGER REQUEST IS ALREADY AWAITING OPERATIONS REVIEW' });

  if (action === 'REQUEST_CANCELLATION') {
    request.managementRequest = {
      id: `MNG-${randomUUID().slice(0, 8).toUpperCase()}`,
      type: 'CANCELLATION',
      status: 'PENDING',
      passengerNumber: passenger.number || 1,
      passengerName: passenger.name || 'Passenger',
      submittedBy: passenger.email || '',
      note: note || 'Passenger requested cancellation review.',
      submittedAt: new Date().toISOString()
    };
    await persistBookingRequest(request);
    await addBookingTimelineEvent(request.id, 'CANCELLATION REQUESTED', `Passenger ${passenger.number || 1}: ${request.managementRequest.note}`);
    void notifyPilotOfBookingManagement(request, 'Cancellation request', request.managementRequest.note).catch(error => console.error('MANAGEMENT EMAIL:', error.message));
    return res.json({ message: 'CANCELLATION REQUEST SENT TO OPERATIONS', booking: bookingManagementView(request, passenger) });
  }

  const normalized = normalizeManagementPatch(req.body);
  if (normalized.error) return res.status(400).json({ error: normalized.error });
  if (currentView.canDirectEdit) {
    applyManagementPatch(request, passenger, normalized.patch);
    await persistBookingRequest(request);
    await addBookingTimelineEvent(request.id, 'PASSENGER UPDATE', `Passenger ${passenger.number || 1} updated permitted booking details. ${managementPatchSummary(normalized.patch)}`);
    void notifyPilotOfBookingManagement(request, 'Passenger booking updated', managementPatchSummary(normalized.patch)).catch(error => console.error('MANAGEMENT EMAIL:', error.message));
    return res.json({ message: 'BOOKING DETAILS UPDATED', booking: bookingManagementView(request, passenger) });
  }

  request.managementRequest = {
    id: `MNG-${randomUUID().slice(0, 8).toUpperCase()}`,
    type: 'CHANGE',
    status: 'PENDING',
    passengerNumber: passenger.number || 1,
    passengerName: passenger.name || 'Passenger',
    submittedBy: passenger.email || '',
    requested: normalized.patch,
    note,
    submittedAt: new Date().toISOString()
  };
  await persistBookingRequest(request);
  await addBookingTimelineEvent(request.id, 'CHANGE REQUESTED', `Passenger ${passenger.number || 1}: ${managementPatchSummary(normalized.patch)}${note ? ` / ${note}` : ''}`);
  void notifyPilotOfBookingManagement(request, 'Passenger change request', `${managementPatchSummary(normalized.patch)}${note ? ` / ${note}` : ''}`).catch(error => console.error('MANAGEMENT EMAIL:', error.message));
  res.json({ message: 'CHANGE REQUEST SENT TO OPERATIONS', booking: bookingManagementView(request, passenger) });
});

app.get('/api/booking-ops/availability', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  res.json({ dates: [...bookingAvailability.entries()].map(([date, item]) => ({ date, ...item })) });
});

app.put('/api/booking-ops/availability/:date', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const date = String(req.params.date || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return res.status(400).json({ error: 'VALID DATE REQUIRED' });
  const isOpen = req.body?.isOpen === true || req.body?.isOpen === 'true';
  const item = {
    isOpen,
    note: normalizeBookingText(req.body?.note, 120),
    updatedAt: new Date().toISOString()
  };
  bookingAvailability.set(date, item);
  await persistAvailability(date, item);
  res.json({ date, ...item });
});

app.get('/api/booking-ops/requests', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({
    requests: bookingRequests.slice().reverse().map(request => ({
      ...request,
      bookingMessage: formatBookingMessage(request)
    }))
  });
});

function reimbursementPdfText(value, limit = 70) {
  const text = String(value || '---').replace(/\s+/g, ' ').trim() || '---';
  return text.length > limit ? `${text.slice(0, Math.max(0, limit - 3))}...` : text;
}

function reimbursementPdfNumber(value) {
  const number = Number(String(value || 0).replace(',', '.').replace(/[^0-9.-]/g, ''));
  return Number.isFinite(number) ? number : 0;
}

function reimbursementPdfMoney(value) {
  return `EUR ${reimbursementPdfNumber(value).toFixed(2)}`;
}

function ofpRound(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  const factor = 10 ** digits;
  return Math.round(number * factor) / factor;
}

function ofpClock(value, minutes = 0) {
  const match = String(value || '').match(/^(\d{1,2}):(\d{2})/);
  if (!match) return '----';
  const total = ((Number(match[1]) * 60) + Number(match[2]) + minutes + 1440) % 1440;
  return `${String(Math.floor(total / 60)).padStart(2, '0')}${String(total % 60).padStart(2, '0')}`;
}

function operationalFlightPlanDefaults(request) {
  const depAirport = bookingAirports.find(item => item.icao === request?.dep);
  const arrAirport = bookingAirports.find(item => item.icao === request?.arr);
  const minutes = Math.max(10, Number(request?.estimatedFlightMinutes) || estimateBoardingPassFlightMinutes(request?.dep, request?.arr) || 60);
  const distance = depAirport && arrAirport && depAirport.icao !== arrAirport.icao
    ? ofpRound(kmToNm(haversineKm(depAirport, arrAirport)), 0)
    : ofpRound((minutes / 60) * AIRCRAFT_PROFILE.cruiseTasKt, 0);
  const fuelFlowGph = 10;
  const tripFuelGal = ofpRound((minutes / 60) * fuelFlowGph, 1);
  const taxiFuelGal = ofpRound(AIRCRAFT_PROFILE.startTaxiTakeoffFuelGal, 1);
  const contingencyFuelGal = ofpRound(Math.max(0.5, tripFuelGal * 0.05), 1);
  const finalReserveFuelGal = ofpRound(fuelFlowGph * 0.5, 1);
  const localOut = ofpClock(request?.requestTime, 0);
  const localOff = ofpClock(request?.requestTime, 10);
  const localOn = ofpClock(request?.requestTime, 10 + minutes);
  const localIn = ofpClock(request?.requestTime, 15 + minutes);
  const commander = request?.crew?.commander && request.crew.commander !== 'UNASSIGNED'
    ? request.crew.commander
    : '';
  return {
    dateUtc: request?.requestDate || '',
    aircraftRegistration: '',
    aircraftModel: 'PA28-200R',
    aircraftCode: 'PA28',
    commanderName: commander,
    commanderPhone: '',
    callsign: 'NGA PRIVATE AVIATION',
    departure: request?.dep || '',
    destination: request?.arr || '',
    depRunway: '',
    arrRunway: '',
    route: request?.dep && request?.arr ? `${request.dep} DCT ${request.arr}` : '',
    clearance: '',
    alternate: '',
    alternateRoute: '',
    flightRules: 'VFR',
    cruiseFlightLevel: 'VFR',
    cruiseSpeedKt: AIRCRAFT_PROFILE.cruiseTasKt,
    distanceNm: distance,
    estimatedEnrouteMinutes: minutes,
    estimatedOut: `----Z/${localOut}L`,
    estimatedOff: `----Z/${localOff}L`,
    estimatedOn: `----Z/${localOn}L`,
    estimatedIn: `----Z/${localIn}L`,
    scheduledOut: '',
    scheduledOff: '',
    scheduledOn: '',
    scheduledIn: '',
    actualOut: '',
    actualOff: '',
    actualOn: '',
    actualIn: '',
    fuelFlowGph,
    taxiFuelGal,
    tripFuelGal,
    contingencyFuelGal,
    alternateFuelGal: 0,
    finalReserveFuelGal,
    extraFuelGal: 0,
    alternateDistanceNm: 0,
    alternateEetMinutes: 0,
    bemLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.basicEmptyWeightLbs,
    bemArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.basicEmptyCgIn,
    crew1Lb: 0,
    crew2Lb: 0,
    passengerWeightOverrideLb: null,
    baggageWeightOverrideLb: null,
    crewArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.cockpit.armIn,
    passengerArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.rearSeats.armIn,
    fuelArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.fuel.armIn,
    baggageArmIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.baggage.armIn,
    baggageLimitLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.baggage.weightLimitLbs,
    fuelCapacityLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.fuel.weightLimitLbs,
    fuelCapacityGal: AIRCRAFT_WEIGHT_BALANCE_PROFILE.stations.fuel.usableGallons,
    gearRetractionMomentChangeInLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.gearRetractionMomentChangeInLb,
    maxZeroFuelWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxZeroFuelWeightLbs,
    maxRampWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxRampWeightLbs,
    maxTakeoffWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxTakeoffWeightLbs,
    maxLandingWeightLb: AIRCRAFT_WEIGHT_BALANCE_PROFILE.maxLandingWeightLbs,
    cgForwardLimitIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.forwardCgLimits.at(-1).cgIn,
    cgAftLimitIn: AIRCRAFT_WEIGHT_BALANCE_PROFILE.aftCgLimits.at(-1).cgIn,
    weightBalanceProfile: AIRCRAFT_WEIGHT_BALANCE_PROFILE,
    performanceSnapshot: null,
    notamReviewAccepted: false,
    airspaceReviewAccepted: false,
    briefingRemarks: '',
    briefingReviewedAt: '',
    stabTrim: '',
    releaseName: commander,
    releaseAccepted: false,
    releaseSnapshot: null
  };
}

function operationalCgLimitAtWeight(points, weightLbs) {
  const weight = Number(weightLbs);
  if (!Number.isFinite(weight) || !points.length) return 0;
  if (weight <= points[0].weightLbs) return points[0].cgIn;
  const last = points.at(-1);
  if (weight >= last.weightLbs) return last.cgIn;
  for (let index = 1; index < points.length; index += 1) {
    const upper = points[index];
    const lower = points[index - 1];
    if (weight <= upper.weightLbs) {
      const ratio = (weight - lower.weightLbs) / (upper.weightLbs - lower.weightLbs);
      return lower.cgIn + ((upper.cgIn - lower.cgIn) * ratio);
    }
  }
  return last.cgIn;
}

function operationalCgEnvelopeStatus(weightLbs, cgIn) {
  const profile = AIRCRAFT_WEIGHT_BALANCE_PROFILE;
  const minimumWeight = profile.forwardCgLimits[0].weightLbs;
  const maximumWeight = profile.forwardCgLimits.at(-1).weightLbs;
  const forwardLimitIn = operationalCgLimitAtWeight(profile.forwardCgLimits, weightLbs);
  const aftLimitIn = operationalCgLimitAtWeight(profile.aftCgLimits, weightLbs);
  const weightInRange = weightLbs >= minimumWeight && weightLbs <= maximumWeight;
  return {
    forwardLimitIn: ofpRound(forwardLimitIn, 2),
    aftLimitIn: ofpRound(aftLimitIn, 2),
    withinEnvelope: weightInRange && cgIn >= forwardLimitIn && cgIn <= aftLimitIn
  };
}

function operationalFlightPlanCalculations(plan, request) {
  const profile = AIRCRAFT_WEIGHT_BALANCE_PROFILE;
  const bookingPassengerWeightLb = ofpRound(getRequestPassengers(request).reduce((sum, passenger) => sum + (Number(passenger.weightKg) || 0), 0) * 2.20462, 1);
  const bookingBaggageWeightLb = request?.carryOnBags === 'YES' ? ofpRound((Number(request.baggageWeightKg) || 0) * 2.20462, 1) : 0;
  const hasPassengerOverride = plan.passengerWeightOverrideLb !== null && plan.passengerWeightOverrideLb !== undefined && Number.isFinite(Number(plan.passengerWeightOverrideLb));
  const hasBaggageOverride = plan.baggageWeightOverrideLb !== null && plan.baggageWeightOverrideLb !== undefined && Number.isFinite(Number(plan.baggageWeightOverrideLb));
  const passengerWeightLb = hasPassengerOverride ? ofpRound(Number(plan.passengerWeightOverrideLb), 1) : bookingPassengerWeightLb;
  const baggageWeightLb = hasBaggageOverride ? ofpRound(Number(plan.baggageWeightOverrideLb), 1) : bookingBaggageWeightLb;
  const blockFuelGal = ofpRound(
    Number(plan.taxiFuelGal || 0)
      + Number(plan.tripFuelGal || 0)
      + Number(plan.contingencyFuelGal || 0)
      + Number(plan.alternateFuelGal || 0)
      + Number(plan.finalReserveFuelGal || 0)
      + Number(plan.extraFuelGal || 0),
    1
  );
  const fuelWeightLb = ofpRound(blockFuelGal * profile.fuelWeightPerGallonLbs, 1);
  const crewWeightLb = ofpRound(Number(plan.crew1Lb || 0) + Number(plan.crew2Lb || 0), 1);
  const zeroFuelWeightLb = ofpRound(profile.basicEmptyWeightLbs + crewWeightLb + passengerWeightLb + baggageWeightLb, 1);
  const rampWeightLb = ofpRound(zeroFuelWeightLb + fuelWeightLb, 1);
  const taxiBurnLb = ofpRound(Number(plan.taxiFuelGal || 0) * profile.fuelWeightPerGallonLbs, 1);
  const tripBurnLb = ofpRound(Number(plan.tripFuelGal || 0) * profile.fuelWeightPerGallonLbs, 1);
  const takeoffWeightLb = ofpRound(Math.max(0, rampWeightLb - taxiBurnLb), 1);
  const landingWeightLb = ofpRound(Math.max(0, takeoffWeightLb - tripBurnLb), 1);
  const bemMoment = profile.basicEmptyWeightLbs * profile.basicEmptyCgIn;
  const crewMoment = crewWeightLb * profile.stations.cockpit.armIn;
  const passengerMoment = passengerWeightLb * profile.stations.rearSeats.armIn;
  const baggageMoment = baggageWeightLb * profile.stations.baggage.armIn;
  const zeroFuelMoment = bemMoment + crewMoment + passengerMoment + baggageMoment;
  const takeoffFuelWeight = Math.max(0, fuelWeightLb - taxiBurnLb);
  const landingFuelWeight = Math.max(0, takeoffFuelWeight - tripBurnLb);
  const takeoffMoment = zeroFuelMoment + (takeoffFuelWeight * profile.stations.fuel.armIn);
  const takeoffGearUpMoment = takeoffMoment + profile.gearRetractionMomentChangeInLb;
  const landingMoment = zeroFuelMoment + (landingFuelWeight * profile.stations.fuel.armIn);
  const zeroFuelCgIn = zeroFuelWeightLb > 0 ? ofpRound(zeroFuelMoment / zeroFuelWeightLb, 2) : 0;
  const takeoffCgIn = takeoffWeightLb > 0 ? ofpRound(takeoffMoment / takeoffWeightLb, 2) : 0;
  const takeoffGearUpCgIn = takeoffWeightLb > 0 ? ofpRound(takeoffGearUpMoment / takeoffWeightLb, 2) : 0;
  const landingCgIn = landingWeightLb > 0 ? ofpRound(landingMoment / landingWeightLb, 2) : 0;
  const takeoffEnvelope = operationalCgEnvelopeStatus(takeoffWeightLb, takeoffCgIn);
  const gearUpEnvelope = operationalCgEnvelopeStatus(takeoffWeightLb, takeoffGearUpCgIn);
  const landingEnvelope = operationalCgEnvelopeStatus(landingWeightLb, landingCgIn);
  const zeroFuelEnvelope = operationalCgEnvelopeStatus(zeroFuelWeightLb, zeroFuelCgIn);
  const warnings = [];
  if (!crewWeightLb) warnings.push('ENTER CREW WEIGHTS');
  if (baggageWeightLb > profile.stations.baggage.weightLimitLbs) warnings.push('BAGGAGE EXCEEDS 200 LB STATION LIMIT');
  if (fuelWeightLb > profile.stations.fuel.weightLimitLbs) warnings.push('FUEL EXCEEDS 288 LB / 48 USG LIMIT');
  if (zeroFuelWeightLb > profile.maxZeroFuelWeightLbs) warnings.push('ZERO FUEL WEIGHT EXCEEDS 2650 LB LIMIT');
  if (rampWeightLb > profile.maxRampWeightLbs) warnings.push('RAMP WEIGHT EXCEEDS 2657 LB LIMIT');
  if (takeoffWeightLb > profile.maxTakeoffWeightLbs) warnings.push('TAKEOFF WEIGHT EXCEEDS 2650 LB LIMIT');
  if (landingWeightLb > profile.maxLandingWeightLbs) warnings.push('LANDING WEIGHT EXCEEDS 2650 LB LIMIT');
  if (!zeroFuelEnvelope.withinEnvelope) warnings.push('ZERO FUEL POINT OUTSIDE APPROVED CG ENVELOPE');
  if (!takeoffEnvelope.withinEnvelope) warnings.push('TAKEOFF POINT OUTSIDE APPROVED CG ENVELOPE');
  if (!gearUpEnvelope.withinEnvelope) warnings.push('GEAR-UP POINT OUTSIDE APPROVED CG ENVELOPE');
  if (!landingEnvelope.withinEnvelope) warnings.push('LANDING POINT OUTSIDE APPROVED CG ENVELOPE');
  if (!String(plan.route || '').trim()) warnings.push('ENTER PILOT ROUTE');
  if (!String(plan.alternate || '').trim()) warnings.push('ALTERNATE REVIEW OPEN');
  if (!plan.notamReviewAccepted) warnings.push('NOTAM REVIEW ACKNOWLEDGEMENT REQUIRED');
  if (!plan.airspaceReviewAccepted) warnings.push('AIRSPACE REVIEW ACKNOWLEDGEMENT REQUIRED');
  if (!plan.performanceSnapshot?.departure?.runway || !plan.performanceSnapshot?.arrival?.runway) warnings.push('CAPTURE RUNWAY AND WEATHER PERFORMANCE INPUTS');
  return {
    bookingPassengerWeightLb,
    bookingBaggageWeightLb,
    passengerWeightLb,
    baggageWeightLb,
    passengerWeightSource: hasPassengerOverride ? 'PILOT ACTUAL LOAD' : 'BOOKING MANIFEST',
    baggageWeightSource: hasBaggageOverride ? 'PILOT ACTUAL LOAD' : 'BOOKING MANIFEST',
    blockFuelGal,
    fuelWeightLb,
    crewWeightLb,
    zeroFuelWeightLb,
    rampWeightLb,
    takeoffWeightLb,
    landingWeightLb,
    zeroFuelCgIn,
    takeoffCgIn,
    takeoffGearUpCgIn,
    landingCgIn,
    forwardCgLimitIn: takeoffEnvelope.forwardLimitIn,
    aftCgLimitIn: takeoffEnvelope.aftLimitIn,
    weightBalanceProfile: profile,
    warnings,
    calculationStatus: warnings.length ? 'REVIEW REQUIRED' : 'CALCULATED / VERIFY POH'
  };
}

function createOperationalReleaseSnapshot(plan, request, actor) {
  return {
    version: 'NGA-OPERATIONAL-RELEASE-2026-07',
    releaseId: `REL-${randomUUID().slice(0, 8).toUpperCase()}`,
    releasedAt: new Date().toISOString(),
    releasedBy: actor,
    bookingReference: request.id,
    route: {
      departure: plan.departure,
      destination: plan.destination,
      depRunway: plan.depRunway,
      arrRunway: plan.arrRunway,
      routing: plan.route,
      alternate: plan.alternate,
      alternateRoute: plan.alternateRoute
    },
    fuel: {
      blockFuelGal: plan.blockFuelGal,
      taxiFuelGal: plan.taxiFuelGal,
      tripFuelGal: plan.tripFuelGal,
      contingencyFuelGal: plan.contingencyFuelGal,
      alternateFuelGal: plan.alternateFuelGal,
      finalReserveFuelGal: plan.finalReserveFuelGal,
      extraFuelGal: plan.extraFuelGal
    },
    massAndBalance: {
      zeroFuelWeightLb: plan.zeroFuelWeightLb,
      rampWeightLb: plan.rampWeightLb,
      takeoffWeightLb: plan.takeoffWeightLb,
      landingWeightLb: plan.landingWeightLb,
      takeoffCgIn: plan.takeoffCgIn,
      gearUpCgIn: plan.takeoffGearUpCgIn,
      landingCgIn: plan.landingCgIn,
      passengerWeightSource: plan.passengerWeightSource,
      baggageWeightSource: plan.baggageWeightSource
    },
    briefing: {
      notamReviewAccepted: plan.notamReviewAccepted,
      airspaceReviewAccepted: plan.airspaceReviewAccepted,
      remarks: plan.briefingRemarks,
      reviewedAt: plan.briefingReviewedAt
    },
    performance: plan.performanceSnapshot
  };
}

function movementTimestamp(now = new Date()) {
  const utc = now.toISOString().slice(11, 16).replace(':', '');
  const localParts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Helsinki',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23'
  }).formatToParts(now);
  const local = Object.fromEntries(localParts.map(part => [part.type, part.value]));
  return `${utc}Z/${local.hour || '--'}${local.minute || '--'}L`;
}

function reimbursementStatementDefaults(request) {
  const flightHours = Math.max(1, Math.round(Number(request.estimatedFlightMinutes || 60) / 60));
  return {
    statementDate: '',
    dueDate: '',
    deliveryDate: '',
    crewName: request.crew?.commander && request.crew.commander !== 'UNASSIGNED' ? request.crew.commander : 'FLIGHT CREW',
    crewEmail: '',
    crewPhone: '',
    passengerNumber: '0',
    payerName: '',
    payerEmail: '',
    payerPhone: '',
    bankName: '',
    accountName: '',
    iban: '',
    bic: '',
    siirtoPhone: '',
    paymentReference: request.id,
    charges: [
      { description: 'Fuel Charge (AVGAS) 100LL', quantity: `${flightHours * 10} US Gal`, unitPrice: '0.00', amount: '0.00' },
      { description: 'Aircraft operating costs', quantity: `${flightHours} (H)`, unitPrice: '400.00', amount: String(flightHours * 400) },
      { description: `Airport Fees (${request.dep || 'DEP'}) DEP/ARR`, quantity: '1', unitPrice: '0.00', amount: '0.00' },
      { description: request.extras ? `Passenger (${request.extras})` : 'Passenger extras', quantity: '1', unitPrice: '0.00', amount: '0.00' }
    ]
  };
}

async function createReimbursementStatementPdf(request) {
  const statement = { ...reimbursementStatementDefaults(request), ...(request.reimbursementStatement || {}) };
  const passengers = getRequestPassengers(request);
  const passengerIndex = Math.max(0, Math.min(passengers.length - 1, Number(statement.passengerNumber) || 0));
  const passenger = passengers[passengerIndex] || passengers[0] || {};
  const payerName = statement.payerName || passenger.name || statement.passengerName || 'PASSENGER';
  const payerEmail = statement.payerEmail || passenger.email || statement.passengerEmail || '---';
  const payerPhone = statement.payerPhone || passenger.phone || statement.passengerPhone || '---';
  const charges = (statement.charges?.length ? statement.charges : reimbursementStatementDefaults(request).charges).slice(0, 8);
  const total = charges.reduce((sum, charge) => sum + reimbursementPdfNumber(charge.amount), 0);
  const dueAmount = total / 2;
  const navy = rgb(3 / 255, 28 / 255, 69 / 255);
  const paleBlue = rgb(231 / 255, 246 / 255, 1);
  const ink = rgb(17 / 255, 17 / 255, 17 / 255);
  const muted = rgb(82 / 255, 82 / 255, 82 / 255);
  const white = rgb(1, 1, 1);
  const pdfDoc = await PDFDocument.create();
  const courier = await pdfDoc.embedFont(StandardFonts.Courier);
  const courierBold = await pdfDoc.embedFont(StandardFonts.CourierBold);
  const titleFont = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const page = pdfDoc.addPage([595.28, 841.89]);
  const draw = (value, x, y, size, font = courier, color = ink, limit = 75) => page.drawText(reimbursementPdfText(value, limit), { x, y, size, font, color });

  page.drawRectangle({ x: 0, y: 0, width: 595.28, height: 841.89, color: white });
  draw('PRIVATE FLIGHT', 48, 792, 25, titleFont, ink, 30);
  draw('REIMBURSEMENT STATEMENT', 48, 756, 12, courierBold, ink, 32);
  draw(`${request.dep || '---'}-${request.arr || '---'}`, 275, 756, 12, courierBold, ink, 20);

  try {
    const logo = await pdfDoc.embedPng(fs.readFileSync(path.join(__dirname, 'public', 'nga-private-aviation-logo.png')));
    const scaled = logo.scaleToFit(115, 95);
    page.drawImage(logo, { x: 432, y: 729, width: scaled.width, height: scaled.height });
  } catch (error) {
    draw('NGA PRIVATE AVIATION', 402, 766, 9, courierBold, navy, 25);
  }

  draw('FLIGHT CREW', 48, 720, 12, courierBold);
  draw(statement.payerName || statement.payerEmail ? 'COST COVERED BY' : 'PASSENGER', 355, 720, 12, courierBold);
  draw(statement.crewName || 'FLIGHT CREW', 48, 692, 12, courierBold, ink, 35);
  draw(statement.crewEmail || '---', 48, 675, 10, courier, muted, 38);
  draw(statement.crewPhone || '---', 48, 660, 10, courier, muted, 38);
  draw(`Date: ${statement.statementDate || '---'}`, 48, 645, 10, courier, muted, 38);
  draw(payerName, 355, 692, 12, courierBold, ink, 32);
  draw(payerEmail, 355, 675, 10, courier, muted, 38);
  draw(payerPhone, 355, 660, 10, courier, muted, 38);
  draw(`Date: ${statement.statementDate || '---'}`, 355, 645, 10, courier, muted, 38);
  page.drawLine({ start: { x: 48, y: 626 }, end: { x: 547, y: 626 }, thickness: 1.2, color: navy });

  draw('Reimbursement of proportional flight costs under EASA NCO.GEN.103.', 115, 596, 10, courier, muted, 70);
  draw('This is not a commercial transport service - Private Flight operated under', 75, 578, 10, courier, muted, 76);
  draw('non-commercial NCO provisions.', 210, 560, 10, courier, muted, 38);

  page.drawRectangle({ x: 48, y: 514, width: 499, height: 31, color: navy });
  draw('DESCRIPTION OF CHARGES', 64, 524, 13, courierBold, white, 35);
  const columns = [48, 282, 352, 447, 547];
  const tableTop = 488;
  const rowHeight = charges.length > 6 ? 20 : charges.length > 4 ? 24 : 30;
  const rowTextOffset = Math.max(7, Math.round((rowHeight - 9) / 2));
  const headers = ['Description', 'Quantity', 'Unit Price (EUR)', 'Amount (EUR)'];
  headers.forEach((header, index) => draw(header, columns[index] + 6, tableTop - 15, 8.5, courierBold, ink, index === 0 ? 32 : 20));
  for (let row = 0; row < charges.length; row += 1) {
    const y = tableTop - 30 - (row * rowHeight);
    page.drawRectangle({ x: 48, y, width: 499, height: rowHeight, color: paleBlue });
    columns.slice(1, -1).forEach(x => page.drawLine({ start: { x, y }, end: { x, y: y + rowHeight }, thickness: 0.8, color: white }));
    page.drawLine({ start: { x: 48, y }, end: { x: 547, y }, thickness: 0.8, color: white });
    if (row < charges.length) {
      const charge = charges[row];
      draw(charge.description, 54, y + rowTextOffset, 8.5, courier, muted, 42);
      draw(charge.quantity, 289, y + rowTextOffset, 8.5, courier, muted, 11);
      draw(reimbursementPdfMoney(charge.unitPrice), 359, y + rowTextOffset, 8.5, courier, muted, 14);
      draw(reimbursementPdfMoney(charge.amount), 454, y + rowTextOffset, 8.5, courier, muted, 14);
    }
  }

  const lowerY = 240;
  draw('DUE DATE (48H)', 50, lowerY + 42, 10, courier);
  draw(`: ${statement.dueDate || '---'}`, 147, lowerY + 42, 10, courier, muted, 28);
  draw('DELIVERY DATE', 50, lowerY + 27, 10, courier);
  draw(`: ${statement.deliveryDate || '---'}`, 147, lowerY + 27, 10, courier, muted, 28);
  draw('Payment must be made within 48 hours of', 50, lowerY - 30, 9, courier, muted, 43);
  draw('the departure date, by bank transfer.', 50, lowerY - 43, 9, courier, muted, 43);

  page.drawRectangle({ x: 314, y: lowerY + 66, width: 233, height: 30, color: navy });
  draw('BANK DETAILS', 343, lowerY + 76, 13, courierBold, white, 25);
  page.drawRectangle({ x: 314, y: lowerY - 53, width: 233, height: 103, color: rgb(.92, .92, .92) });
  draw(`Bank: ${statement.bankName || '---'}`, 326, lowerY + 30, 10, courier, muted, 31);
  draw(`Account Name: ${statement.accountName || '---'}`, 326, lowerY + 14, 10, courier, muted, 31);
  draw(`IBAN: ${statement.iban || '---'}`, 326, lowerY - 2, 10, courier, muted, 31);
  draw(`BIC: ${statement.bic || '---'}`, 326, lowerY - 18, 10, courier, muted, 31);
  draw(`Payment Reference: ${statement.paymentReference || request.id}`, 326, lowerY - 34, 9, courier, muted, 34);
  page.drawLine({ start: { x: 48, y: 165 }, end: { x: 547, y: 165 }, thickness: 1.2, color: navy });
  draw('CHARGES:', 335, 140, 12, courierBold, ink, 16);
  draw(String(charges.length), 420, 140, 12, courierBold, ink, 4);
  draw('PER PASSENGER:', 335, 117, 12, courierBold, ink, 20);
  draw(reimbursementPdfMoney(dueAmount / Math.max(1, Number(request.seats || passengers.length || 1))), 440, 117, 10, courier, muted, 16);
  draw('TOTAL FLIGHT COST:', 335, 94, 12, courierBold, ink, 25);
  draw(reimbursementPdfMoney(total), 467, 94, 10, courierBold, ink, 14);
  draw('DUE AMOUNT:', 335, 71, 12, courierBold, ink, 19);
  draw(reimbursementPdfMoney(dueAmount), 440, 71, 10, courierBold, ink, 16);
  draw('PAGE 1/1', 490, 34, 9, courier, muted, 12);
  return Buffer.from(await pdfDoc.save());
}

function ofpPdfText(value, fallback = '.....', limit = 90) {
  const text = String(value ?? '').replace(/\s+/g, ' ').trim() || fallback;
  return text.length > limit ? `${text.slice(0, Math.max(0, limit - 3))}...` : text;
}

function ofpPdfDuration(minutes) {
  const total = Math.max(0, Math.round(Number(minutes) || 0));
  return `${String(Math.floor(total / 60)).padStart(2, '0')}${String(total % 60).padStart(2, '0')}`;
}

async function createOperationalFlightPlanPdf(request) {
  const plan = normalizeOperationalFlightPlan(request.operationalFlightPlan || operationalFlightPlanDefaults(request), request);
  const wbProfile = AIRCRAFT_WEIGHT_BALANCE_PROFILE;
  const pdfDoc = await PDFDocument.create();
  pdfDoc.registerFontkit(fontkit);
  const courier = await pdfDoc.embedFont(fs.readFileSync(path.join(__dirname, 'cour.ttf')), { subset: true });
  const courierBold = await pdfDoc.embedFont(StandardFonts.CourierBold);
  const sansBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const ink = rgb(20 / 255, 20 / 255, 20 / 255);
  const muted = rgb(96 / 255, 96 / 255, 96 / 255);
  const navy = rgb(3 / 255, 28 / 255, 69 / 255);
  const green = rgb(25 / 255, 190 / 255, 23 / 255);
  const lightGrid = rgb(210 / 255, 210 / 255, 210 / 255);
  const width = 595.5;
  const height = 842.25;
  const templateImages = await Promise.all([1, 2, 3].map(pageNumber => (
    pdfDoc.embedPng(fs.readFileSync(path.join(__dirname, 'data', `ofp-template-page-${pageNumber}.png`)))
  )));
  const pages = templateImages.map(image => {
    const page = pdfDoc.addPage([width, height]);
    page.drawImage(image, { x: 0, y: 0, width, height });
    return page;
  });
  const [page1, page2, page3] = pages;
  const clean = (value, fallback = '......', max = 96) => ofpPdfText(value, fallback, max).toUpperCase();
  const fitSize = (font, value, size, maxWidth) => {
    if (!maxWidth) return size;
    const measured = font.widthOfTextAtSize(value, size);
    return measured > maxWidth ? Math.max(6.8, size * (maxWidth / measured)) : size;
  };
  const drawTop = (page, value, x, top, size = 10.8, options = {}) => {
    const font = options.font || courier;
    const text = clean(value, options.fallback ?? '......', options.maxChars || 96);
    const actualSize = fitSize(font, text, size, options.maxWidth);
    page.drawText(text, { x, y: height - top - actualSize, size: actualSize, font, color: options.color || ink });
    return actualSize;
  };
  const drawRightTop = (page, value, right, top, size = 10.8, options = {}) => {
    const font = options.font || courier;
    const text = clean(value, options.fallback ?? '......', options.maxChars || 96);
    const actualSize = fitSize(font, text, size, options.maxWidth);
    page.drawText(text, {
      x: right - font.widthOfTextAtSize(text, actualSize),
      y: height - top - actualSize,
      size: actualSize,
      font,
      color: options.color || ink
    });
  };
  const formatDate = value => {
    const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})/);
    return match ? `${match[3]}/${match[2]}/${match[1]}` : 'DD/MM/YYYY';
  };
  const utcPart = value => String(value || '').toUpperCase().match(/\d{4}Z/)?.[0] || '----Z';
  const timeValue = value => clean(value, '......Z', 18);
  const fuelTime = minutes => {
    const total = Math.max(0, Math.round(Number(minutes) || 0));
    return `${String(Math.floor(total / 60)).padStart(total >= 60 ? 2 : 1, '0')}.${String(total % 60).padStart(2, '0')}`;
  };
  const fuelMinutes = gallons => (Number(gallons || 0) / Math.max(Number(plan.fuelFlowGph || 0), 0.1)) * 60;
  const airportDisplay = code => {
    const airport = bookingAirports.find(item => item.icao === code);
    return airport?.short ? `${code}/${airport.short}` : code;
  };
  const registration = plan.aircraftRegistration || 'REG TBD';
  const model = plan.aircraftModel || 'PA28-200R';
  const routePair = `${plan.departure || 'DEP'}-${plan.destination || 'ARR'}`;
  const headerDate = `${formatDate(plan.dateUtc)}/${utcPart(plan.estimatedOff || plan.scheduledOff)}`;
  const pageFooter = (page, pageNumber, top = 785.7) => {
    drawTop(page, `${registration}  - ${model}`, pageNumber === 3 ? 242.2 : 253.3, top, 11, { maxWidth: 160 });
    drawTop(page, `Page ${pageNumber} of 3`, pageNumber === 3 ? 468.8 : 463.5, pageNumber === 3 ? 762.8 : 797.8, 10.5, { maxWidth: 82, fallback: '' });
  };

  drawTop(page1, registration, 48.6, 28.7, 11.2, { font: courierBold, maxWidth: 68 });
  drawTop(page1, model, 126.3, 28.7, 11.2, { font: courierBold, maxWidth: 88 });
  drawTop(page1, routePair, 226.7, 28.7, 11.2, { font: courierBold, maxWidth: 86 });
  drawTop(page1, headerDate, 321.2, 28.7, 11.2, { font: courierBold, maxWidth: 120 });
  drawTop(page1, `${registration} ${formatDate(plan.dateUtc)}`, 51, 92, 10.5, { maxWidth: 120 });
  drawTop(page1, `${model} ${plan.aircraftCode || 'PA28'}`, 51, 104, 10.5, { maxWidth: 122 });
  drawTop(page1, `ICAO FIN ${registration}`, 51, 116, 10.5, { maxWidth: 125 });
  drawRightTop(page1, `${airportDisplay(plan.departure)} - ${airportDisplay(plan.destination)}`, 570.6, 87.2, 10.5, { maxWidth: 142 });
  drawRightTop(page1, `(PIC)-${plan.commanderName || 'NOT ASSIGNED'}`, 570.6, 99.2, 10.5, { maxWidth: 142 });
  drawRightTop(page1, `FIN ${plan.commanderPhone || 'PHONE NOT ENTERED'}`, 570.6, 111.2, 10.5, { maxWidth: 142 });
  drawRightTop(page1, plan.callsign || 'NGA PRIVATE AVIATION', 570.6, 123.2, 10.5, { maxWidth: 142 });
  const timeRows = [
    [plan.estimatedOut, plan.scheduledOut, plan.actualOut],
    [plan.estimatedOff, plan.scheduledOff, plan.actualOff],
    [plan.estimatedOn, plan.scheduledOn, plan.actualOn],
    [plan.estimatedIn, plan.scheduledIn, plan.actualIn]
  ];
  [199.7, 224.8, 250, 275].forEach((top, index) => {
    drawTop(page1, timeValue(timeRows[index][0]), 147.6, top, 10.5, { maxWidth: 78 });
    drawTop(page1, timeValue(timeRows[index][1]), 260, top, 10.5, { maxWidth: 78 });
    drawTop(page1, timeValue(timeRows[index][2]), 378.7, top, 10.5, { maxWidth: 54 });
  });
  drawTop(page1, ofpPdfDuration(plan.estimatedEnrouteMinutes + 15), 147.6, 300.2, 10.5, { maxWidth: 42 });
  drawTop(page1, '......', 378.7, 300.2, 10.5, { maxWidth: 42 });
  drawTop(page1, 'BRIEF', 189.1, 393.2, 10.5, { fallback: '' });
  drawTop(page1, 'FL', 237.2, 393.2, 10.5, { fallback: '' });
  drawTop(page1, String(plan.cruiseFlightLevel || 'VFR').replace(/^FL/, ''), 275.1, 393.2, 10.5, { maxWidth: 28 });
  drawTop(page1, 'SPD', 313.7, 393.2, 10.5, { fallback: '' });
  drawTop(page1, plan.cruiseSpeedKt, 340.2, 393.2, 10.5, { maxWidth: 30 });
  drawTop(page1, 'EET', 382.4, 393.2, 10.5, { fallback: '' });
  drawTop(page1, plan.estimatedEnrouteMinutes, 418.4, 393.2, 10.5, { maxWidth: 28 });
  drawTop(page1, 'ICAO', 237.2, 405.2, 10.5, { fallback: '' });
  drawTop(page1, plan.flightRules || 'VFR', 270.1, 405.2, 10.5, { maxWidth: 28 });
  drawTop(page1, 'DIST', 307.7, 405.2, 10.5, { fallback: '' });
  drawTop(page1, `${plan.distanceNm}NM`, 343.3, 405.2, 10.5, { maxWidth: 36 });
  drawTop(page1, 'BURN', 382.4, 405.2, 10.5, { fallback: '' });
  drawTop(page1, plan.tripFuelGal, 420.1, 405.2, 10.5, { maxWidth: 28 });
  const routeText = clean(plan.route || `${plan.departure} DCT ${plan.destination}`, 'PILOT ROUTE REQUIRED', 68);
  drawTop(page1, `DEP ${plan.departure} RWY ${plan.depRunway || 'TBD'} ROUTE ${routeText}`, 49.5, 457.6, 10.2, { maxWidth: 480 });
  drawTop(page1, `ARR ${plan.destination} RWY ${plan.arrRunway || 'TBD'}`, 49.5, 471.1, 10.2, { maxWidth: 475 });
  if (plan.clearance) drawTop(page1, plan.clearance, 49.5, 526, 10.2, { maxWidth: 475, maxChars: 76 });
  drawTop(page1, '---------------------', 207, 588.1, 10.5, { fallback: '' });
  drawTop(page1, `ALTN     ${airportDisplay(plan.alternate || 'TBD')}     FL${String(plan.cruiseFlightLevel || 'VFR').replace(/^FL/, '')}   DIS ${plan.alternateDistanceNm}NM  EET ${plan.alternateEetMinutes}MIN  BURN ${plan.alternateFuelGal}`, 48.6, 601.2, 10.2, { maxWidth: 470 });
  drawTop(page1, `ROUTE ${plan.alternateRoute || 'PILOT ALTERNATE REVIEW REQUIRED'}`, 41.4, 619, 10.2, { maxWidth: 490, maxChars: 78 });
  pageFooter(page1, 1);

  drawTop(page2, registration, 58.2, 26.5, 11.2, { font: courierBold, maxWidth: 72 });
  drawTop(page2, model, 141.2, 26.7, 11.2, { font: courierBold, maxWidth: 82 });
  drawTop(page2, 'ARPT', 169.7, 136.1, 10.5, { fallback: '' });
  drawTop(page2, 'FUEL', 219.5, 136.1, 10.5, { fallback: '' });
  drawTop(page2, 'TIME', 264.9, 136, 10.5, { fallback: '' });
  drawTop(page2, 'ACTUAL', 480.3, 136.3, 10.5, { fallback: '' });
  const fuelRows = [
    [plan.departure, plan.taxiFuelGal, fuelMinutes(plan.taxiFuelGal), 156.4],
    [plan.destination, plan.tripFuelGal, plan.estimatedEnrouteMinutes, 172.8],
    ['', plan.contingencyFuelGal, fuelMinutes(plan.contingencyFuelGal), 188.1],
    [plan.alternate || 'TBD', plan.alternateFuelGal, plan.alternateEetMinutes, 203.9],
    ['', plan.finalReserveFuelGal, fuelMinutes(plan.finalReserveFuelGal), 220.5],
    ['', plan.extraFuelGal, fuelMinutes(plan.extraFuelGal), 236.6]
  ];
  fuelRows.forEach(([airport, gallons, minutes, top]) => {
    if (airport) drawTop(page2, airport, 176.8, top, 10.5, { maxWidth: 34 });
    drawTop(page2, gallons, 219.5, top, 10.5, { maxWidth: 32 });
    drawTop(page2, fuelTime(minutes), 266.3, top, 10.5, { maxWidth: 34 });
  });
  drawTop(page2, plan.blockFuelGal, 214, 267, 10.5, { maxWidth: 38 });
  drawTop(page2, fuelTime(plan.blockFuelGal / Math.max(plan.fuelFlowGph, 0.1) * 60), 266.3, 267, 10.5, { maxWidth: 42 });
  const departureFuelLb = ofpRound(Math.max(0, plan.fuelWeightLb - (plan.taxiFuelGal * wbProfile.fuelWeightPerGallonLbs)), 1);
  const tripBurnLb = ofpRound(plan.tripFuelGal * wbProfile.fuelWeightPerGallonLbs, 1);
  const arrivalFuelLb = ofpRound(Math.max(0, departureFuelLb - tripBurnLb), 1);
  drawTop(page2, `${plan.fuelWeightLb} LB`, 480.3, 161.6, 10.5, { maxWidth: 62 });
  drawTop(page2, `${departureFuelLb} LB`, 480.3, 186.3, 10.5, { maxWidth: 62 });
  drawTop(page2, `${tripBurnLb} LB`, 480.3, 211.7, 10.5, { maxWidth: 62 });
  drawTop(page2, `${arrivalFuelLb} LB`, 480.3, 237.1, 10.5, { maxWidth: 62 });
  const passengerCount = getRequestPassengers(request).length;
  const manifestRows = [
    [plan.releaseAccepted ? '1' : '0', '', plan.releaseAccepted ? utcPart(plan.actualOut || plan.estimatedOut) : '......', 367.3],
    [wbProfile.basicEmptyWeightLbs, '-----', wbProfile.basicEmptyWeightLbs, 392.4],
    [passengerCount, '3', passengerCount, 417.8],
    [plan.baggageWeightLb, wbProfile.stations.baggage.weightLimitLbs, plan.baggageWeightLb, 441.2],
    [plan.zeroFuelWeightLb, wbProfile.maxZeroFuelWeightLbs, plan.zeroFuelWeightLb, 467.6],
    [plan.fuelWeightLb, wbProfile.stations.fuel.weightLimitLbs, plan.fuelWeightLb, 491.1],
    [plan.rampWeightLb, wbProfile.maxRampWeightLbs, plan.rampWeightLb, 514.7],
    [plan.takeoffWeightLb, wbProfile.maxTakeoffWeightLbs, plan.takeoffWeightLb, 539.9],
    [plan.stabTrim || '------', '+/-5', plan.stabTrim || '......', 565],
    [plan.landingWeightLb, wbProfile.maxLandingWeightLbs, plan.landingWeightLb, 590.1]
  ];
  manifestRows.forEach(([estimated, maximum, actual, top]) => {
    drawTop(page2, estimated, 147.9, top, 10.2, { maxWidth: 48 });
    if (String(maximum || '').trim()) drawTop(page2, maximum, 214.3, top, 10.2, { maxWidth: 42 });
    drawTop(page2, actual, 260.6, top, 10.2, { maxWidth: 48 });
  });
  drawTop(page2, 'DISPATCHER, EFIN HELSINKI', 56.2, 669.7, 10.5, { fallback: '', maxWidth: 190 });
  drawTop(page2, `CAPTAIN ${plan.releaseAccepted ? (plan.releaseName || plan.commanderName || 'SIGNED') : '..............'}`, 56.2, 702.4, 10.5, { maxWidth: 170 });
  drawTop(page2, `PIC ${plan.commanderName || '...............'}`, 241.3, 702.4, 10.5, { maxWidth: 145 });
  pageFooter(page2, 2, 762.8);

  drawTop(page3, registration, 55.1, 25.7, 11.2, { font: courierBold, maxWidth: 72 });
  drawTop(page3, model, 141.2, 26.7, 11.2, { font: courierBold, maxWidth: 82 });
  const takeoffFuelLb = ofpRound(Math.max(0, plan.fuelWeightLb - (plan.taxiFuelGal * wbProfile.fuelWeightPerGallonLbs)), 1);
  const wbRows = [
    [wbProfile.basicEmptyWeightLbs, wbProfile.basicEmptyWeightLbs, wbProfile.basicEmptyCgIn, ofpRound(wbProfile.basicEmptyWeightLbs * wbProfile.basicEmptyCgIn, 2), 133.7],
    [plan.crewWeightLb, '----', wbProfile.stations.cockpit.armIn, ofpRound(plan.crewWeightLb * wbProfile.stations.cockpit.armIn, 2), 160.7],
    [plan.passengerWeightLb, '----', wbProfile.stations.rearSeats.armIn, ofpRound(plan.passengerWeightLb * wbProfile.stations.rearSeats.armIn, 2), 187.8],
    [takeoffFuelLb, wbProfile.stations.fuel.weightLimitLbs, wbProfile.stations.fuel.armIn, ofpRound(takeoffFuelLb * wbProfile.stations.fuel.armIn, 2), 214.8],
    [plan.baggageWeightLb, wbProfile.stations.baggage.weightLimitLbs, wbProfile.stations.baggage.armIn, ofpRound(plan.baggageWeightLb * wbProfile.stations.baggage.armIn, 2), 241.8],
    ['----', '----', '------', wbProfile.gearRetractionMomentChangeInLb, 268.9]
  ];
  wbRows.forEach(([weight, maximum, arm, moment, top]) => {
    drawTop(page3, weight, 163.7, top, 10.5, { maxWidth: 52 });
    drawTop(page3, maximum, 227.7, top, 10.5, { maxWidth: 42 });
    drawTop(page3, arm, 277.4, top, 10.5, { maxWidth: 45 });
    drawTop(page3, moment, 334.6, top, 10.5, { maxWidth: 67 });
  });
  drawTop(page3, plan.takeoffWeightLb, 163.7, 295.9, 10.5, { maxWidth: 52 });
  drawTop(page3, wbProfile.maxTakeoffWeightLbs, 227.7, 295.9, 10.5, { maxWidth: 42 });
  drawTop(page3, 'TOTAL', 277.4, 295.9, 10.5, { fallback: '' });
  drawTop(page3, '=', 406.8, 295.9, 10.5, { fallback: '' });
  drawTop(page3, `${plan.takeoffCgIn} IN`, 424.6, 295.9, 10.5, { maxWidth: 72 });

  const graph = { x: 164, y: 166, w: 295, h: 310 };
  const plot = { x: 178, y: 185, w: 268, h: 282 };
  page3.drawRectangle({ x: graph.x, y: graph.y, width: graph.w, height: graph.h, borderColor: lightGrid, borderWidth: 1.2 });
  const minCg = 80;
  const maxCg = 93;
  const minWeight = 1400;
  const maxWeight = 2700;
  const graphX = cg => plot.x + ((cg - minCg) / (maxCg - minCg)) * plot.w;
  const graphY = weight => plot.y + ((weight - minWeight) / (maxWeight - minWeight)) * plot.h;
  for (let cg = minCg; cg <= maxCg; cg += 1) {
    const x = graphX(cg);
    page3.drawLine({ start: { x, y: plot.y }, end: { x, y: plot.y + plot.h }, thickness: cg % 2 === 0 ? 0.5 : 0.25, color: lightGrid });
    if (cg % 2 === 0) page3.drawText(String(cg), { x: x - 5, y: plot.y - 15, size: 6.5, font: sansBold, color: ink });
  }
  for (let weight = minWeight; weight <= 2600; weight += 100) {
    const y = graphY(weight);
    page3.drawLine({ start: { x: plot.x, y }, end: { x: plot.x + plot.w, y }, thickness: weight % 200 === 0 ? 0.5 : 0.25, color: lightGrid });
    if (weight % 200 === 0) page3.drawText(String(weight), { x: plot.x - 31, y: y - 2.5, size: 6.5, font: sansBold, color: ink });
  }
  const envelope = [
    ...wbProfile.forwardCgLimits.map(point => [point.cgIn, point.weightLbs]),
    ...[...wbProfile.aftCgLimits].reverse().map(point => [point.cgIn, point.weightLbs]),
    [wbProfile.forwardCgLimits[0].cgIn, wbProfile.forwardCgLimits[0].weightLbs]
  ];
  envelope.slice(0, -1).forEach((point, index) => {
    const next = envelope[index + 1];
    page3.drawLine({ start: { x: graphX(point[0]), y: graphY(point[1]) }, end: { x: graphX(next[0]), y: graphY(next[1]) }, thickness: 1.4, color: ink });
  });
  const zeroFuelMoment = (wbProfile.basicEmptyWeightLbs * wbProfile.basicEmptyCgIn)
    + (plan.crewWeightLb * wbProfile.stations.cockpit.armIn)
    + (plan.passengerWeightLb * wbProfile.stations.rearSeats.armIn)
    + (plan.baggageWeightLb * wbProfile.stations.baggage.armIn);
  const zeroFuelCg = plan.zeroFuelWeightLb > 0 ? zeroFuelMoment / plan.zeroFuelWeightLb : 0;
  const zfwX = graphX(zeroFuelCg);
  const zfwY = graphY(plan.zeroFuelWeightLb);
  const towX = graphX(plan.takeoffCgIn);
  const towY = graphY(plan.takeoffWeightLb);
  if (Number.isFinite(zfwX) && Number.isFinite(zfwY)) {
    page3.drawLine({ start: { x: zfwX, y: zfwY + 6 }, end: { x: zfwX + 6, y: zfwY }, thickness: 1, color: muted });
    page3.drawLine({ start: { x: zfwX + 6, y: zfwY }, end: { x: zfwX, y: zfwY - 6 }, thickness: 1, color: muted });
    page3.drawLine({ start: { x: zfwX, y: zfwY - 6 }, end: { x: zfwX - 6, y: zfwY }, thickness: 1, color: muted });
    page3.drawLine({ start: { x: zfwX - 6, y: zfwY }, end: { x: zfwX, y: zfwY + 6 }, thickness: 1, color: muted });
    page3.drawText('ZFM', { x: zfwX + 9, y: zfwY - 3, size: 6.5, font: sansBold, color: muted });
  }
  if (Number.isFinite(towX) && Number.isFinite(towY)) {
    page3.drawRectangle({ x: towX - 6, y: towY - 6, width: 12, height: 12, borderColor: green, borderWidth: 1.6 });
    page3.drawText('STRUCTURAL MTOM', { x: Math.max(plot.x + 120, towX - 92), y: Math.min(plot.y + plot.h - 8, towY + 9), size: 6.2, font: sansBold, color: ink });
  }
  page3.drawText('CG (in)', { x: graph.x + 135, y: graph.y - 14, size: 6.5, font: sansBold, color: ink });
  page3.drawText('Mass (lbs)', { x: graph.x - 24, y: graph.y + 126, size: 6.5, font: sansBold, color: ink, rotate: degrees(90) });
  pageFooter(page3, 3, 762.8);

  return Buffer.from(await pdfDoc.save());
}

async function createSignedAgreementPdf(request, passenger) {
  const pdfDoc = await PDFDocument.create();
  const courier = await pdfDoc.embedFont(StandardFonts.Courier);
  const courierBold = await pdfDoc.embedFont(StandardFonts.CourierBold);
  const titleFont = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const page = pdfDoc.addPage([595.28, 841.89]);
  const navy = rgb(3 / 255, 28 / 255, 69 / 255);
  const green = rgb(30 / 255, 97 / 255, 59 / 255);
  const paleGreen = rgb(234 / 255, 247 / 255, 239 / 255);
  const ink = rgb(17 / 255, 17 / 255, 17 / 255);
  const muted = rgb(82 / 255, 82 / 255, 82 / 255);
  const pageWidth = 595.28;
  const left = 48;
  const right = 547;
  let y = 790;
  const draw = (value, x, yPos, size, font = courier, color = ink, limit = 90) => page.drawText(reimbursementPdfText(value, limit), { x, y: yPos, size, font, color });
  const drawWrapped = (value, size = 8.7, indent = 0, color = muted) => {
    const words = String(value || '').split(/\s+/).filter(Boolean);
    const maxWidth = right - left - indent;
    let line = '';
    for (const word of words) {
      const next = line ? `${line} ${word}` : word;
      if (courier.widthOfTextAtSize(next, size) > maxWidth && line) {
        draw(line, left + indent, y, size, courier, color, 160);
        y -= size + 4;
        line = word;
      } else {
        line = next;
      }
    }
    if (line) {
      draw(line, left + indent, y, size, courier, color, 160);
      y -= size + 4;
    }
  };
  const detail = (label, value) => {
    draw(label.toUpperCase(), left, y, 8.3, courierBold, navy, 26);
    draw(value || 'Not provided', 202, y, 8.7, courier, ink, 62);
    y -= 18;
  };

  page.drawRectangle({ x: 0, y: 0, width: pageWidth, height: 841.89, color: rgb(1, 1, 1) });
  try {
    const logo = await pdfDoc.embedPng(fs.readFileSync(path.join(__dirname, 'public', 'nga-private-aviation-logo.png')));
    const scaled = logo.scaleToFit(120, 84);
    page.drawImage(logo, { x: 421, y: 742, width: scaled.width, height: scaled.height });
  } catch {
    draw('NGA PRIVATE AVIATION', 398, 775, 9, courierBold, navy, 28);
  }
  draw('PRIVATE FLIGHT', left, y, 11, courierBold, navy, 24);
  y -= 30;
  draw('FLIGHT AGREEMENT', left, y, 26, titleFont, navy, 32);
  y -= 25;
  draw(`REFERENCE ${request.id}`, left, y, 13, courierBold, navy, 34);
  y -= 28;
  page.drawRectangle({ x: left, y: y - 4, width: 202, height: 25, color: paleGreen, borderColor: green, borderWidth: 1 });
  draw('ELECTRONICALLY SIGNED', left + 10, y + 5, 10, courierBold, green, 28);
  y -= 30;
  page.drawLine({ start: { x: left, y }, end: { x: right, y }, thickness: 1.2, color: navy });
  y -= 24;

  draw('PASSENGER DETAILS', left, y, 11, courierBold, navy, 32);
  y -= 20;
  detail('Passenger', `${passenger.title || ''} ${passenger.name || 'Passenger'}`.trim());
  detail('Date of birth', passenger.dob || 'Not provided');
  detail('Passport country', passenger.passportCountry || 'Not provided');
  detail('Passport / ID', maskedPassport(passenger));
  y -= 4;
  draw('FLIGHT DETAILS', left, y, 11, courierBold, navy, 32);
  y -= 20;
  detail('Route', `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`);
  detail('Date', emailDate(request.requestDate));
  detail('Boarding', `${boardingPassBoardingTime(request.requestTime)} Local Time`);
  detail('Departure', `${formatBoardingPassTime(request.requestTime)} Local Time`);
  detail('Aircraft', PUBLIC_AIRCRAFT_TYPE);
  y -= 4;
  draw('AGREEMENT TERMS', left, y, 11, courierBold, navy, 32);
  y -= 19;
  const terms = [
    'Private operation. This is a private, non-commercial NCO flight. The flight remains subject to the Pilot-in-Command\'s final decision, weather, aircraft serviceability, passenger fitness, baggage loading, weight and balance, applicable rules, and operational limits.',
    'Pilot authority. The Pilot-in-Command has sole authority over the aircraft and may delay, change, cancel, or decline the flight for safety or operational reasons. All passengers must follow lawful pilot instructions.',
    'Safety and fitness. You must disclose any health, medication, alcohol, substance, or other condition that could affect safe flight before boarding. You must not board if unwell or impaired. There is no supplemental oxygen on board.',
    'Conduct and baggage. Disruptive conduct, dangerous goods, undeclared lithium batteries, weapons, explosives, flammable materials, or any item that may affect flight safety are not permitted. Baggage remains subject to pilot approval and aircraft loading limits.',
    'Payment and changes. A Reimbursement Statement may be issued after operational confirmation. Payment, cancellation, and refund arrangements remain subject to the information provided by operations for this booking.'
  ];
  for (const term of terms) {
    drawWrapped(term, 8.1, 8, muted);
    y -= 4;
  }
  y -= 2;
  page.drawRectangle({ x: left, y: y - 54, width: right - left, height: 58, color: paleGreen, borderColor: green, borderWidth: 1 });
  draw('ELECTRONIC ACKNOWLEDGEMENT', left + 10, y - 10, 9.5, courierBold, green, 38);
  draw(`SIGNED BY: ${passenger.agreementSignatureName || passenger.name || 'Passenger'}`, left + 10, y - 26, 8.7, courierBold, ink, 72);
  draw(`SIGNED AT: ${passenger.agreementSignedAt ? new Date(passenger.agreementSignedAt).toISOString().replace('T', ' ').replace('.000Z', ' UTC') : 'Not recorded'}`, left + 10, y - 40, 8.3, courier, ink, 75);
  draw(`AGREEMENT VERSION: ${passenger.agreementVersion || 'PRIVATE-FLIGHT-AGREEMENT-2026-06-24'}`, left + 10, y - 51, 7.5, courier, muted, 86);
  draw('PAGE 1/1', 490, 34, 9, courier, muted, 12);
  return Buffer.from(await pdfDoc.save());
}

app.get('/api/booking-ops/requests/:id/reimbursement-statement/pdf', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  const pdf = await createReimbursementStatementPdf(request);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="${request.id}-REIMBURSEMENT-STATEMENT.pdf"`);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.send(pdf);
});

app.get('/api/booking-ops/requests/:id/operational-flight-plan', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  const operationalFlightPlan = normalizeOperationalFlightPlan(
    request.operationalFlightPlan || operationalFlightPlanDefaults(request),
    request
  );
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({ request, operationalFlightPlan, saved: Boolean(request.operationalFlightPlan) });
});

app.get('/api/booking-ops/requests/:id/operational-flight-plan/pdf', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  if (!request.operationalFlightPlan) return res.status(400).json({ error: 'SAVE THE OPERATIONAL FLIGHT PLAN DRAFT FIRST' });
  const pdf = await createOperationalFlightPlanPdf(request);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="${request.id}-OPERATIONAL-FLIGHT-PLAN.pdf"`);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.send(pdf);
});

app.post('/api/booking-ops/requests/:id/movements/:phase', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  if (!requireFlightCrew(req, res)) return;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  if (!request.operationalFlightPlan) return res.status(409).json({ error: 'SAVE AND RELEASE THE OFP BEFORE RECORDING MOVEMENT TIMES' });

  const phase = String(req.params.phase || '').trim().toLowerCase();
  const movementSteps = [
    { phase: 'out', field: 'actualOut', label: 'OUT' },
    { phase: 'off', field: 'actualOff', label: 'OFF' },
    { phase: 'on', field: 'actualOn', label: 'ON' },
    { phase: 'in', field: 'actualIn', label: 'IN' }
  ];
  const stepIndex = movementSteps.findIndex(item => item.phase === phase);
  if (stepIndex === -1) return res.status(400).json({ error: 'INVALID MOVEMENT PHASE' });

  const previousPlan = normalizeOperationalFlightPlan(request.operationalFlightPlan, request);
  if (!previousPlan.releaseAccepted) return res.status(409).json({ error: 'PILOT RELEASE IS REQUIRED BEFORE RECORDING OUT TIME' });
  const step = movementSteps[stepIndex];
  if (previousPlan[step.field]) return res.status(409).json({ error: `${step.label} TIME IS LOCKED` });
  if (stepIndex > 0 && !previousPlan[movementSteps[stepIndex - 1].field]) {
    return res.status(409).json({ error: `RECORD ${movementSteps[stepIndex - 1].label} BEFORE ${step.label}` });
  }

  const timestamp = movementTimestamp();
  const flightPlan = normalizeOperationalFlightPlan({ ...previousPlan, [step.field]: timestamp }, request);
  request.operationalFlightPlan = flightPlan;
  await persistBookingRequest(request);
  await addBookingTimelineEvent(request.id, 'MOVEMENT TIME LOCKED', `${step.label} ${timestamp}`, pilotActor(req));
  res.json({
    request: {
      ...request,
      bookingMessage: formatBookingMessage(request)
    },
    movement: { phase: step.label, timestamp }
  });
});

app.get('/api/booking-ops/requests/:id/agreement/:passengerNumber/pdf', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  const passengerNumber = Number(req.params.passengerNumber);
  const passenger = getRequestPassengers(request).find(item => Number(item.number || 1) === passengerNumber);
  if (!passenger?.agreementSignedAt) return res.status(404).json({ error: 'SIGNED AGREEMENT NOT AVAILABLE' });
  const pdf = await createSignedAgreementPdf(request, passenger);
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="${request.id}-PAX${passenger.number || 1}-SIGNED-AGREEMENT.pdf"`);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.send(pdf);
});

app.get('/api/booking-ops/requests/:id/pdf', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  const pdfDoc = await PDFDocument.create();
  const font = await pdfDoc.embedFont(StandardFonts.Courier);
  const bold = await pdfDoc.embedFont(StandardFonts.CourierBold);
  const page = pdfDoc.addPage([595.28, 841.89]);
  page.drawRectangle({ x: 0, y: 0, width: 595.28, height: 841.89, color: rgb(1, 1, 1) });
  page.drawText('PRIVATE FLIGHT OPERATIONS REQUEST', { x: 42, y: 795, size: 16, font: bold, color: rgb(0.04, 0.05, 0.12) });
  let y = 765;
  for (const line of formatBookingMessage(request).split('\n')) {
    page.drawText(line.slice(0, 90), { x: 42, y, size: 9.5, font: line.startsWith('PRIVATE') || line.startsWith('REF ') ? bold : font, color: rgb(0.08, 0.08, 0.08) });
    y -= 13;
    if (y < 45) break;
  }
  const pdf = Buffer.from(await pdfDoc.save());
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="${request.id}-OPERATIONS-REQUEST.pdf"`);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.send(pdf);
});

app.post('/api/booking-ops/requests/:id/time-proposal', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  const date = normalizeBookingText(req.body?.date, 20);
  const time = normalizeBookingText(req.body?.time, 16);
  const note = normalizeBookingText(req.body?.note, 300);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return res.status(400).json({ error: 'VALID PROPOSED DATE REQUIRED' });
  if (!/^\d{1,2}:\d{2}$/.test(time)) return res.status(400).json({ error: 'VALID PROPOSED TIME REQUIRED' });
  request.timeProposal = {
    token: createTimeProposalToken(),
    date,
    time,
    note,
    originalDate: request.requestDate || '',
    originalTime: request.requestTime || '',
    responseStatus: 'PENDING',
    responseMessage: '',
    proposedAt: new Date().toISOString(),
    respondedAt: ''
  };
  await persistBookingRequest(request);
  await addBookingTimelineEvent(request.id, 'NEW DEPARTURE TIME PROPOSED', `${proposedTimeLabel(request.timeProposal)}${note ? ` / ${note}` : ''}`, pilotActor(req));
  res.json({
    request: {
      ...request,
      bookingMessage: formatBookingMessage(request)
    }
  });
  void notifyBookerOfTimeProposal(request)
    .then(count => {
      if (count) return addBookingTimelineEvent(request.id, 'TIME PROPOSAL EMAIL', `Departure time proposal sent to ${count} passenger email${count === 1 ? '' : 's'}.`);
      return null;
    })
    .catch(err => console.error('TIME PROPOSAL EMAIL:', err.message));
});

app.patch('/api/booking-ops/requests/:id', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  const actor = pilotActor(req);

  const paymentStatus = String(req.body?.paymentStatus || '').trim().toUpperCase();
  const pilotDecision = String(req.body?.pilotDecision || '').trim().toUpperCase();
  const wasApproved = request.pilotDecision === 'APPROVED' && request.status === 'CONFIRMED';

  if (pilotDecision === 'APPROVED' && !request.reimbursementStatement) {
    return res.status(400).json({ error: 'SAVE THE REIMBURSEMENT STATEMENT BEFORE CONFIRMING THIS FLIGHT' });
  }

  if (paymentStatus) {
    const allowedPayments = new Set(['UNPAID', 'DEPOSIT PAID', 'PAID', 'REFUNDED']);
    if (!allowedPayments.has(paymentStatus)) return res.status(400).json({ error: 'INVALID PAYMENT STATUS' });
    request.paymentStatus = paymentStatus;
    await addBookingTimelineEvent(request.id, 'PAYMENT STATUS', paymentStatus, actor);
  }

  if (pilotDecision) {
    const allowedDecisions = new Set(['PENDING', 'APPROVED', 'NEEDS INFO', 'DECLINED']);
    if (!allowedDecisions.has(pilotDecision)) return res.status(400).json({ error: 'INVALID PILOT DECISION' });
    request.pilotDecision = pilotDecision;
    if (pilotDecision === 'APPROVED') {
      request.status = 'CONFIRMED';
      ensureAgreementTokens(request);
      ensureReimbursementStatementToken(request);
    }
    if (pilotDecision === 'NEEDS INFO') request.status = 'NEEDS INFO';
    if (pilotDecision === 'DECLINED') request.status = 'DECLINED';
    if (pilotDecision === 'PENDING') request.status = 'REQUESTED';
    await addBookingTimelineEvent(request.id, 'PILOT DECISION', pilotDecision, actor);
  }

  const timelineNote = normalizeBookingText(req.body?.timelineNote, 300);
  if (timelineNote) await addBookingTimelineEvent(request.id, 'OPS NOTE', timelineNote, actor);
  if (req.body?.crew && typeof req.body.crew === 'object') {
    const allowedCrew = new Set(['ANTONI GARCIA', 'SAUL GARCIA', 'UNASSIGNED', 'NONE']);
    const commander = normalizeBookingText(req.body.crew.commander, 40).toUpperCase();
    const secondary = normalizeBookingText(req.body.crew.secondary, 40).toUpperCase();
    if (!allowedCrew.has(commander) || !allowedCrew.has(secondary) || commander === 'NONE') {
      return res.status(400).json({ error: 'INVALID CREW ASSIGNMENT' });
    }
    request.crew = { commander, secondary };
    await addBookingTimelineEvent(request.id, 'CREW ASSIGNMENT', `CMD ${commander} / SIC ${secondary}`, actor);
  }
  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'additionalEmailContacts')) {
    request.additionalEmailContacts = normalizeAdditionalEmailContacts(req.body.additionalEmailContacts);
    await addBookingTimelineEvent(
      request.id,
      'EMAIL CONTACTS UPDATED',
      request.additionalEmailContacts.length ? request.additionalEmailContacts.join(', ') : 'Additional contacts cleared.',
      actor
    );
  }
  if (req.body?.itinerary && typeof req.body.itinerary === 'object') {
    const itineraryUpdate = normalizeBookingItineraryUpdate(req.body.itinerary, request.tripType || 'ONE_WAY');
    if (itineraryUpdate.error) return res.status(400).json({ error: itineraryUpdate.error });
    const itineraryNote = applyBookingItineraryUpdate(request, itineraryUpdate);
    request.bookingCorrectedAt = new Date().toISOString();
    await addBookingTimelineEvent(request.id, 'ITINERARY CORRECTED', itineraryNote, actor);
  }
  if (req.body?.reimbursementStatement && typeof req.body.reimbursementStatement === 'object') {
    const statement = normalizeReimbursementStatement(req.body.reimbursementStatement);
    if (statement) {
      request.reimbursementStatement = statement;
      ensureReimbursementStatementToken(request);
      await addBookingTimelineEvent(request.id, 'REIMBURSEMENT STATEMENT SAVED', 'Statement draft updated in Pilot Ops.', actor);
    }
  }
  if (req.body?.operationalFlightPlan && typeof req.body.operationalFlightPlan === 'object') {
    const previousPlan = request.operationalFlightPlan
      ? normalizeOperationalFlightPlan(request.operationalFlightPlan, request)
      : null;
    const flightPlan = normalizeOperationalFlightPlan(req.body.operationalFlightPlan, request);
    if (flightPlan) {
      const briefingComplete = flightPlan.notamReviewAccepted && flightPlan.airspaceReviewAccepted;
      const briefingWasComplete = previousPlan?.notamReviewAccepted && previousPlan?.airspaceReviewAccepted;
      flightPlan.briefingReviewedAt = briefingComplete
        ? (briefingWasComplete ? previousPlan.briefingReviewedAt : new Date().toISOString())
        : '';
      if (flightPlan.releaseAccepted && req.pilotSession?.role !== 'COMMANDER') {
        return res.status(403).json({ error: 'COMMANDER ACCESS REQUIRED TO RELEASE AN OFP' });
      }
      if (flightPlan.releaseAccepted && flightPlan.warnings.length) {
        return res.status(400).json({ error: `OFP RELEASE BLOCKED: ${flightPlan.warnings.join(' / ')}` });
      }
      const wasReleased = Boolean(previousPlan?.releaseAccepted);
      if (flightPlan.releaseAccepted && !wasReleased) {
        flightPlan.releaseSnapshot = createOperationalReleaseSnapshot(flightPlan, request, actor);
        await addBookingTimelineEvent(
          request.id,
          'OFP RELEASE SNAPSHOT',
          `${flightPlan.releaseSnapshot.releaseId} / ${flightPlan.departure}-${flightPlan.destination} / TOW ${flightPlan.takeoffWeightLb}LB / BLOCK ${flightPlan.blockFuelGal}USG`,
          actor
        );
      } else {
        flightPlan.releaseSnapshot = previousPlan?.releaseSnapshot || null;
      }
      request.operationalFlightPlan = flightPlan;
      await addBookingTimelineEvent(
        request.id,
        flightPlan.releaseAccepted ? 'OFP RELEASE ACKNOWLEDGED' : wasReleased ? 'OFP RELEASE CLEARED' : 'OFP DRAFT SAVED',
        `${flightPlan.departure}-${flightPlan.destination} / ${flightPlan.calculationStatus}`,
        actor
      );
    }
  }
  await persistBookingRequest(request);

  res.json({
    request: {
      ...request,
      bookingMessage: formatBookingMessage(request)
    }
  });
  if (pilotDecision === 'APPROVED' && !wasApproved && !request.confirmationEmailSentAt) {
    void deliverApprovalNotification(request);
  }
});

app.post('/api/booking-ops/requests/:id/management-request', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  const managementRequest = request.managementRequest;
  if (!managementRequest || managementRequest.status !== 'PENDING') return res.status(409).json({ error: 'NO PENDING PASSENGER MANAGEMENT REQUEST' });
  const decision = String(req.body?.decision || '').trim().toUpperCase();
  if (!['APPLY', 'DECLINE'].includes(decision)) return res.status(400).json({ error: 'SELECT APPLY OR DECLINE' });
  const passenger = getRequestPassengers(request).find(item => Number(item.number || 1) === Number(managementRequest.passengerNumber || 1)) || getRequestPassengers(request)[0];
  const isCancellation = managementRequest.type === 'CANCELLATION';

  if (decision === 'APPLY') {
    if (isCancellation) {
      request.status = 'CANCELLED';
      request.pilotDecision = 'CANCELLED';
      request.cancelledAt = new Date().toISOString();
    } else if (managementRequest.requested) {
      applyManagementPatch(request, passenger, managementRequest.requested);
    }
    managementRequest.status = 'APPLIED';
  } else {
    managementRequest.status = 'DECLINED';
  }
  managementRequest.resolvedAt = new Date().toISOString();
  managementRequest.resolvedBy = 'FLIGHT OPERATIONS';
  await persistBookingRequest(request);
  const outcome = decision === 'APPLY' ? (isCancellation ? 'CANCELLATION CONFIRMED' : 'CHANGE APPLIED') : 'REQUEST DECLINED';
  await addBookingTimelineEvent(request.id, 'MANAGEMENT REQUEST DECISION', `${outcome} / ${managementRequest.type}`);

  const recipient = normalizeBookingEmail(managementRequest.submittedBy || passenger?.email);
  if (recipient) {
    const emailHtml = privateFlightEmailHtml({
      status: outcome,
      reference: request.id,
      greeting: emailPassengerName(passenger),
      intro: isCancellation && decision === 'APPLY'
        ? 'Operations has confirmed the cancellation of this flight request.'
        : decision === 'APPLY'
          ? 'Operations has reviewed and applied your requested booking changes.'
          : 'Operations reviewed the request but could not apply it to the current flight file.',
      details: [
        ['Route', `${boardingPassAirportLabel(request.dep, request.depName)} to ${boardingPassAirportLabel(request.arr, request.arrName)}`],
        ['Date', emailDate(request.requestDate)],
        ['Departure', `${formatBoardingPassTime(request.requestTime)} Local Time`],
        ['Request', managementRequest.type],
        ['Outcome', outcome]
      ],
      closing: ['Best regards,', 'NGA Private Aviation Team', 'info.ngaprivateaviation@gmail.com']
    });
    void sendBookingEmail({
      to: recipient,
      subject: `${outcome} / ${request.id}`,
      text: `${outcome}\nReference: ${request.id}\nRoute: ${request.depName || request.dep} to ${request.arrName || request.arr}\n\nNGA Private Aviation Team`,
      html: emailHtml
    }).catch(error => console.error('MANAGEMENT DECISION EMAIL:', error.message));
  }

  res.json({ request: { ...request, bookingMessage: formatBookingMessage(request) } });
});

app.delete('/api/booking-ops/requests/:id', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const requestId = String(req.params.id || '').trim().toUpperCase();
  const index = bookingRequests.findIndex(item => item.id === requestId);
  if (index === -1) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  bookingRequests.splice(index, 1);
  bookingTimeline.delete(requestId);
  await removeBookingRequest(requestId);
  res.json({ ok: true, id: requestId });
});

app.get('/api/booking-ops/requests/:id/timeline', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const requestId = String(req.params.id || '').trim().toUpperCase();
  res.json({ events: bookingTimeline.get(requestId) || [] });
});

app.get('/api/booking-ops/requests/:id/wallet-pass/:passengerNumber', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  if (!isBoardingPassReady(request)) {
    return res.status(409).json({ error: 'PILOT APPROVAL REQUIRED BEFORE WALLET PASS GENERATION' });
  }
  const passengerNumber = Number(req.params.passengerNumber);
  const passengers = getRequestPassengers(request);
  const passenger = passengers.find(item => Number(item.number) === passengerNumber);
  if (!passenger) return res.status(404).json({ error: 'PASSENGER NOT FOUND' });

  try {
    const { buffer } = await createWalletPass(request, passenger);
    await addBookingTimelineEvent(request.id, 'APPLE WALLET PASS GENERATED', `Passenger ${passenger.number || 1}`);
    res.setHeader('Content-Type', 'application/vnd.apple.pkpass');
    res.setHeader('Content-Disposition', `attachment; filename="${request.id}-PAX${passenger.number || 1}.pkpass"`);
    res.setHeader('Cache-Control', 'no-store');
    res.send(buffer);
  } catch (err) {
    console.error('WALLET PASS:', err.message);
    res.status(err.code === 'WALLET_NOT_CONFIGURED' ? 503 : 500).json({ error: err.message || 'WALLET PASS GENERATION FAILED' });
  }
});

app.post('/api/booking-ops/requests/:id/boarding-pass/:passengerNumber/link', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });
  if (!isBoardingPassReady(request)) {
    return res.status(409).json({ error: 'PILOT APPROVAL REQUIRED BEFORE ISSUING A BOARDING PASS' });
  }
  const passengerNumber = Number(req.params.passengerNumber);
  const passengers = getRequestPassengers(request);
  const passenger = passengers.find(item => Number(item.number) === passengerNumber);
  if (!passenger) return res.status(404).json({ error: 'PASSENGER NOT FOUND' });

  if (!passenger.boardingPassToken) {
    passenger.boardingPassToken = createBoardingPassToken();
    request.passengers = passengers;
    await persistBookingRequest(request);
    await addBookingTimelineEvent(request.id, 'WEB BOARDING PASS ISSUED', `Passenger ${passenger.number || 1}`);
  }

  res.json({
    url: `/private-flight-information-pass/${encodeURIComponent(request.id)}/${passenger.boardingPassToken}`,
    passenger: passenger.number || 1
  });
});

app.get('/api/boarding-passes/:token', async (req, res) => {
  await bookingStoreReady;
  const token = String(req.params.token || '').trim();
  const request = bookingRequests.find(item => getRequestPassengers(item).some(passenger => passenger.boardingPassToken === token));
  if (!request || !isBoardingPassReady(request)) return res.status(404).json({ error: 'BOARDING PASS NOT AVAILABLE' });
  const passenger = getRequestPassengers(request).find(item => item.boardingPassToken === token);
  if (!passenger) return res.status(404).json({ error: 'BOARDING PASS NOT AVAILABLE' });
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({ pass: publicBoardingPassView(request, passenger) });
});

app.get('/api/pass-check/:token', async (req, res) => {
  await bookingStoreReady;
  const token = String(req.params.token || '').trim();
  const request = bookingRequests.find(item => getRequestPassengers(item).some(passenger => passenger.boardingPassToken === token));
  if (!request || !isBoardingPassReady(request)) return res.status(404).json({ error: 'PASS NOT VALID' });
  const passenger = getRequestPassengers(request).find(item => item.boardingPassToken === token);
  if (!passenger) return res.status(404).json({ error: 'PASS NOT VALID' });
  const pass = publicBoardingPassView(request, passenger);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json({
    verification: {
      status: pass.status,
      statusTone: pass.statusTone,
      reference: pass.reference,
      passenger: pass.passenger,
      route: `${pass.from} to ${pass.to}`,
      date: pass.date,
      boardingTime: pass.boardingTime,
      seat: pass.seat,
      baggage: pass.baggage,
      gate: pass.gate
    }
  });
});

app.get('/api/boarding-passes/:token/calendar.ics', async (req, res) => {
  await bookingStoreReady;
  const token = String(req.params.token || '').trim();
  const request = bookingRequests.find(item => getRequestPassengers(item).some(passenger => passenger.boardingPassToken === token));
  if (!request || !isBoardingPassReady(request)) return res.status(404).end();
  const passenger = getRequestPassengers(request).find(item => item.boardingPassToken === token);
  const calendar = passenger ? buildBoardingPassCalendar(request, passenger) : null;
  if (!calendar) return res.status(400).json({ error: 'FLIGHT DATE AND TIME REQUIRED' });
  res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${request.id}-PAX${passenger.number || 1}.ics"`);
  res.setHeader('Cache-Control', 'no-store');
  res.send(calendar);
});

app.get('/api/boarding-passes/:token/qr', async (req, res) => {
  await bookingStoreReady;
  const token = String(req.params.token || '').trim();
  const request = bookingRequests.find(item => getRequestPassengers(item).some(passenger => passenger.boardingPassToken === token));
  if (!request || !isBoardingPassReady(request)) return res.status(404).end();
  const passenger = getRequestPassengers(request).find(item => item.boardingPassToken === token);
  if (!passenger) return res.status(404).end();

  try {
    const QRCode = (await import('qrcode')).default;
    const passUrl = `${PUBLIC_SITE_URL}/pass-check/${encodeURIComponent(token)}`;
    const svg = await QRCode.toString(passUrl, {
      type: 'svg',
      margin: 1,
      color: { dark: '#ffffff', light: '#16125e' }
    });
    res.setHeader('Content-Type', 'image/svg+xml; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(svg);
  } catch (err) {
    console.error('BOARDING PASS QR:', err.message);
    res.status(500).end();
  }
});

app.post('/api/booking-requests', async (req, res) => {
  await bookingStoreReady;
  const flight = costShareFlights.find(item => item.id === String(req.body?.flightId || ''));
  const [depAirport, arrAirport] = await Promise.all([
    getBookingAirport(req.body?.dep || flight?.dep),
    getBookingAirport(req.body?.arr || flight?.arr)
  ]);
  if (!depAirport || !arrAirport) return res.status(400).json({ error: 'VALID DEPARTURE AND DESTINATION REQUIRED' });

  const seats = normalizeBookingSeats(req.body?.seats);
  const view = flight ? publicFlightView(flight) : null;
  if (view && view.seatsAvailable > 0 && seats > view.seatsAvailable) {
    return res.status(400).json({ error: `ONLY ${view.seatsAvailable} SEAT(S) AVAILABLE` });
  }

  const submittedPassengers = Array.isArray(req.body?.passengers) ? req.body.passengers : [];
  const passengers = submittedPassengers.length ? submittedPassengers.slice(0, seats).map((passenger, index) => ({
    number: index + 1,
    title: normalizeBookingText(passenger?.title, 8).toUpperCase(),
    name: normalizeBookingText(passenger?.name, 60),
    email: normalizeBookingEmail(passenger?.email),
    phone: normalizeBookingText(passenger?.phone, 40),
    dob: normalizeBookingText(passenger?.dob, 20),
    weightKg: normalizeBookingText(passenger?.weightKg, 8),
    passportCountry: normalizeBookingText(passenger?.passportCountry, 40),
    nationalId: normalizeBookingText(passenger?.nationalId, 60),
    boardingPassToken: createBoardingPassToken(),
    agreementToken: createAgreementToken(),
    managementToken: createBookingManagementToken()
  })) : [{
    number: 1,
    title: normalizeBookingText(req.body?.title, 8).toUpperCase(),
    name: normalizeBookingText(req.body?.name, 60),
    email: normalizeBookingEmail(req.body?.email),
    phone: normalizeBookingText(req.body?.phone, 40),
    dob: normalizeBookingText(req.body?.dob, 20),
    weightKg: normalizeBookingText(req.body?.weightKg, 8),
    passportCountry: normalizeBookingText(req.body?.passportCountry, 40),
    nationalId: normalizeBookingText(req.body?.nationalId, 60),
    boardingPassToken: createBoardingPassToken(),
    agreementToken: createAgreementToken(),
    managementToken: createBookingManagementToken()
  }];

  while (passengers.length < seats) {
    passengers.push({ number: passengers.length + 1, title: '', name: '', email: '', phone: '', dob: '', weightKg: '', passportCountry: '', nationalId: '', boardingPassToken: createBoardingPassToken(), agreementToken: createAgreementToken(), managementToken: createBookingManagementToken() });
  }

  const leadPassenger = passengers[0] || {};
  const name = leadPassenger.name;
  const email = leadPassenger.email;
  const phone = leadPassenger.phone;
  const message = normalizeBookingText(req.body?.message, 180);
  const requestDate = normalizeBookingText(req.body?.requestDate, 20);
  const requestTime = normalizeBookingText(req.body?.requestTime, 16);
  const tripType = req.body?.tripType === 'ROUNDTRIP' ? 'ROUNDTRIP' : 'ONE_WAY';
  const itineraryUpdate = normalizeBookingItineraryUpdate(req.body, tripType);
  if (itineraryUpdate.error) return res.status(400).json({ error: itineraryUpdate.error });
  const returnDate = tripType === 'ROUNDTRIP' ? normalizeBookingText(req.body?.returnDate, 20) : '';
  const returnTime = tripType === 'ROUNDTRIP' ? normalizeBookingText(req.body?.returnTime, 16) : '';
  const returnPlan = tripType === 'ROUNDTRIP' ? normalizeBookingText(req.body?.returnPlan, 60) : '';
  const dob = leadPassenger.dob;
  const weightKg = leadPassenger.weightKg;
  const nationalId = leadPassenger.nationalId;
  const emergencyName = normalizeBookingText(req.body?.emergencyName, 60);
  const emergencyPhone = normalizeBookingText(req.body?.emergencyPhone, 40);
  const carryOnBags = normalizeBookingText(req.body?.carryOnBags, 8);
  const baggageWeightKg = normalizeBookingText(req.body?.baggageWeightKg, 8);
  const bagType = normalizeBookingText(req.body?.bagType, 60);
  const powerBanks = normalizeBookingText(req.body?.powerBanks, 8);
  const seatPreference = normalizeBookingText(req.body?.seatPreference, 40);
  const extras = Array.isArray(req.body?.extras)
    ? req.body.extras.map(item => normalizeBookingText(item, 40)).filter(Boolean).join(' / ')
    : normalizeBookingText(req.body?.extras, 160);
  const extrasNotesInput = normalizeBookingText(req.body?.extrasNotes, 140);
  const alcoholPreferences = normalizeBookingText(req.body?.alcoholPreferences, 260);
  const extrasNotes = [extrasNotesInput, alcoholPreferences].filter(Boolean).join(' / ');
  const isHankoRoute = depAirport.icao === 'EFHN' || arrAirport.icao === 'EFHN';
  const regattaProfile = isHankoRoute ? normalizeBookingText(req.body?.regattaProfile, 40) : '';
  const regattaArrival = isHankoRoute ? normalizeBookingText(req.body?.regattaArrival, 40) : '';
  const regattaTransfer = isHankoRoute ? normalizeBookingText(req.body?.regattaTransfer, 60) : '';
  const regattaGear = isHankoRoute ? normalizeBookingText(req.body?.regattaGear, 40) : '';
  const regattaNotes = isHankoRoute ? normalizeBookingText(req.body?.regattaNotes, 140) : '';
  const medicalStatus = normalizeBookingText(req.body?.medicalStatus, 80);
  const substancesStatus = normalizeBookingText(req.body?.substancesStatus, 80);
  const flightPurpose = normalizeBookingText(req.body?.flightPurpose, 40);
  const scheduleFlexibility = normalizeBookingText(req.body?.scheduleFlexibility, 60);
  const contractAccepted = req.body?.contractAccepted === true || req.body?.contractAccepted === 'true';

  if (!name || !email || !email.includes('@')) {
    return res.status(400).json({ error: 'LEAD PASSENGER NAME AND VALID EMAIL REQUIRED' });
  }

  const missingPassenger = passengers.find(passenger => (
    !['MR', 'MS', 'MX', 'DR'].includes(passenger.title) ||
    !passenger.name ||
    !passenger.email ||
    !passenger.email.includes('@') ||
    !passenger.phone ||
    !passenger.dob ||
    !passenger.weightKg ||
    !passenger.passportCountry ||
    !passenger.nationalId
  ));
  if (missingPassenger) {
    return res.status(400).json({ error: `PASSENGER ${missingPassenger.number} DETAILS INCOMPLETE` });
  }

  if (!requestDate || !requestTime || !weightKg) {
    return res.status(400).json({ error: 'DATE, ETD, AND WEIGHT REQUIRED' });
  }

  if (tripType === 'ROUNDTRIP' && (!returnDate || !returnTime)) {
    return res.status(400).json({ error: 'RETURN DATE AND TIME REQUIRED FOR ROUNDTRIP' });
  }

  if (carryOnBags === 'YES' && (!baggageWeightKg || baggageWeightKg === 'N/A')) {
    return res.status(400).json({ error: 'BAGGAGE WEIGHT REQUIRED WHEN BAGS ARE YES' });
  }

  if (!contractAccepted) {
    return res.status(400).json({ error: 'CONTRACT AGREEMENT MUST BE ACCEPTED' });
  }
  const priceEstimate = estimateBookingPrice(depAirport, arrAirport, seats, itineraryUpdate.tripType);

  const request = {
    id: nextBookingReference(depAirport.icao, arrAirport.icao),
    flightId: flight?.id || `NG-RQ-${Date.now().toString().slice(-5)}`,
    flightTitle: flight?.title || itineraryUpdate.flightTitle,
    route: `${depAirport.icao}-${arrAirport.icao}`,
    dep: depAirport.icao,
    arr: arrAirport.icao,
    depName: depAirport.name,
    arrName: arrAirport.name,
    aircraft: flight?.aircraft || `${AIRCRAFT_PROFILE.registration} / ${AIRCRAFT_PROFILE.type}`,
    costPerSeatEur: flight?.costPerSeatEur || priceEstimate.perPassengerEur,
    estimatedTotalEur: flight?.costPerSeatEur ? flight.costPerSeatEur * seats : priceEstimate.totalEur,
    priceNote: flight?.costPerSeatEur ? 'PUBLISHED SHARED-COST FLIGHT' : priceEstimate.note,
    requestDate,
    requestTime,
    tripType: itineraryUpdate.tripType,
    flightExperience: itineraryUpdate.flightExperience,
    scenicVariant: itineraryUpdate.scenicVariant,
    estimatedFlightMinutes: itineraryUpdate.estimatedFlightMinutes,
    returnDate: itineraryUpdate.flightExperience ? '' : returnDate,
    returnTime: itineraryUpdate.flightExperience ? '' : returnTime,
    returnPlan: itineraryUpdate.flightExperience ? '' : returnPlan,
    seats,
    passengers,
    name,
    email,
    phone,
    dob,
    weightKg,
    nationalId,
    emergencyName,
    emergencyPhone,
    carryOnBags,
    baggageWeightKg,
    bagType,
    powerBanks,
    seatPreference,
    extras,
    extrasNotes,
    regattaProfile,
    regattaArrival,
    regattaTransfer,
    regattaGear,
    regattaNotes,
    medicalStatus,
    substancesStatus,
    flightPurpose,
    scheduleFlexibility,
    contractAccepted,
    signatureProcessedAt: new Date().toISOString(),
    pilotDecision: 'PENDING',
    paymentStatus: 'UNPAID',
    message,
    status: !view || view.seatsAvailable > 0 ? 'REQUESTED' : 'WAITLIST',
    createdAt: new Date().toISOString()
  };

  bookingRequests.push(request);
  await persistBookingRequest(request);
  await addBookingTimelineEvent(request.id, 'REQUEST RECEIVED', 'Booker submitted request.');

  res.status(201).json({
    request: {
      id: request.id,
      status: request.status,
      pilotDecision: request.pilotDecision
    },
    confirmationText: 'REQUEST RECEIVED. PILOT CONFIRMATION REQUIRED BEFORE ANY FLIGHT IS BOOKED.'
  });
  void deliverBookingNotifications(request);
});

app.get('/api/airport-runways', async (req, res) => {
  try {
    const icao = normalizeIcao(req.query.icao);
    if (!icao) {
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
      return res.json([]);
    }

    const runways = await getAirportRunways(icao);

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json(runways);
  } catch (err) {
    console.error(err);
    res.status(500).json([]);
  }
});

app.get('/api/booking-ops/weather', requirePilotAccess, async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);
    if (!dep || !arr) return res.status(400).json({ error: 'DEPARTURE AND ARRIVAL ARE REQUIRED' });

    const [depWx, arrWx, altnWx, depAirport, arrAirport, altnAirport, depWinds, arrWinds] = await Promise.all([
      getAirportWeather(dep),
      getAirportWeather(arr),
      altn ? getAirportWeather(altn) : null,
      findAirportByIcao(dep),
      findAirportByIcao(arr),
      altn ? findAirportByIcao(altn) : null,
      getWindsAloftForAirport(dep),
      getWindsAloftForAirport(arr)
    ]);

    const summarize = (icao, airport, wx) => {
      if (!icao || !wx) return null;
      const metar = wx.metar && wx.metar !== 'NOT AVAILABLE' ? wx.metar : '';
      const elevationFt = Number.isFinite(airport?.elevationFt) ? Math.round(airport.elevationFt) : null;
      const oatC = parseTemperatureC(metar);
      const qnhHpa = parseQnhHpa(metar);
      return {
        icao,
        name: airport?.name || icao,
        elevationFt,
        mode: wx.mode,
        metar: wx.metar,
        taf: wx.taf,
        metarSource: wx.metarSource,
        tafSource: wx.tafSource,
        metarFallback: wx.metarFallback,
        tafFallback: wx.tafFallback,
        metarDistanceNm: wx.metarDistanceNm,
        tafDistanceNm: wx.tafDistanceNm,
        wind: parseWindDetailed(metar),
        visibilityM: parseVisibility(metar),
        ceilingFt: parseCloudBase(metar),
        oatC,
        qnhHpa,
        densityAltitudeFt: computeDensityAltitudeFt(elevationFt, oatC, qnhHpa)
      };
    };

    const needsAlternate = arrivalNeedsAlternate(arrWx);
    const suggestedAlternate = needsAlternate ? await getSuggestedAlternate(arr, altn) : null;
    const reportOption = (name, fallback = true) => req.query[name] === undefined ? fallback : parseBoolean(req.query[name]);
    const dispatchData = buildDispatchData({
      dep,
      arr,
      altn,
      flight: req.query.flight || '',
      route: req.query.route || `${dep}-${arr}`,
      includeDepMetar: reportOption('includeDepMetar'),
      includeDepTaf: reportOption('includeDepTaf'),
      includeArrMetar: reportOption('includeArrMetar'),
      includeArrTaf: reportOption('includeArrTaf'),
      includeAltnMetar: Boolean(altn) && reportOption('includeAltnMetar'),
      includeAltnTaf: Boolean(altn) && reportOption('includeAltnTaf'),
      includeWindsAloft: reportOption('includeWindsAloft')
    }, depWx, arrWx, altnWx, { dep: depWinds, arr: arrWinds });

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      generatedAt: new Date().toISOString(),
      route: { dep, arr, altn },
      departure: summarize(dep, depAirport, depWx),
      arrival: summarize(arr, arrAirport, arrWx),
      alternate: summarize(altn, altnAirport, altnWx),
      windsAloft: { departure: depWinds, arrival: arrWinds },
      reportText: buildDispatchText(dispatchData),
      operational: {
        remarks: generateAutoRemark(dispatchData),
        arrivalNeedsAlternate: needsAlternate,
        suggestedAlternate
      }
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'EFB WEATHER DATA UNAVAILABLE' });
  }
});

app.get('/api/dispatch-pdf', async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);
    const includeWindsAloft = parseBoolean(req.query.includeWindsAloft);

    const [depWx, arrWx, altnWx, depWinds, arrWinds] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null,
      includeWindsAloft && dep ? getWindsAloftForAirport(dep) : null,
      includeWindsAloft && arr ? getWindsAloftForAirport(arr) : null
    ]);

    const windsAloft = includeWindsAloft ? { dep: depWinds, arr: arrWinds } : null;
    const data = buildDispatchData(req.query, depWx, arrWx, altnWx, windsAloft);
    const pdf = await dispatchToPdfBuffer(data);

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'attachment; filename="ACARS_DISPATCH_REPORT.pdf"');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(pdf);
  } catch (err) {
    console.error(err);
    res.status(500).send('ERROR');
  }
});

app.get('/api/dispatch-text', async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);
    const includeWindsAloft = parseBoolean(req.query.includeWindsAloft);

    const [depWx, arrWx, altnWx, depWinds, arrWinds] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null,
      includeWindsAloft && dep ? getWindsAloftForAirport(dep) : null,
      includeWindsAloft && arr ? getWindsAloftForAirport(arr) : null
    ]);

    const windsAloft = includeWindsAloft ? { dep: depWinds, arr: arrWinds } : null;
    const data = buildDispatchData(req.query, depWx, arrWx, altnWx, windsAloft);
    const reportText = buildDispatchText(data);

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.send(reportText);
  } catch (err) {
    console.error(err);
    res.status(500).send('FAILED TO BUILD REPORT');
  }
});

app.get('/api/dispatch-status', async (req, res) => {
  try {
    const dep = normalizeIcao(req.query.dep);
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);

    const includeDepMetar = parseBoolean(req.query.includeDepMetar);
    const includeDepTaf = parseBoolean(req.query.includeDepTaf);
    const includeArrMetar = parseBoolean(req.query.includeArrMetar);
    const includeArrTaf = parseBoolean(req.query.includeArrTaf);
    const includeAltnMetar = parseBoolean(req.query.includeAltnMetar);
    const includeAltnTaf = parseBoolean(req.query.includeAltnTaf);

    const [depWx, arrWx, altnWx] = await Promise.all([
      dep ? getAirportWeather(dep) : null,
      arr ? getAirportWeather(arr) : null,
      altn ? getAirportWeather(altn) : null
    ]);

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      dep: summarizeAirportStatus(depWx, dep, includeDepMetar, includeDepTaf),
      arr: summarizeAirportStatus(arrWx, arr, includeArrMetar, includeArrTaf),
      altn: summarizeAirportStatus(altnWx, altn, includeAltnMetar, includeAltnTaf)
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      dep: { airport: '', mode: 'NO DATA', label: '' },
      arr: { airport: '', mode: 'NO DATA', label: '' },
      altn: { airport: '', mode: 'NO DATA', label: '' }
    });
  }
});

app.get('/api/dispatch-advice', async (req, res) => {
  try {
    const arr = normalizeIcao(req.query.arr);
    const altn = normalizeIcao(req.query.altn);

    const arrWx = arr ? await getAirportWeather(arr) : null;
    const needsAlternate = arrivalNeedsAlternate(arrWx);
    const suggestedAlternate = needsAlternate ? await getSuggestedAlternate(arr, altn) : null;

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json({
      arrivalNeedsAlternate: needsAlternate,
      suggestedAlternate
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      arrivalNeedsAlternate: false,
      suggestedAlternate: null
    });
  }
});

app.get('/api/performance', async (req, res) => {
  try {
    const data = await buildPerformanceData(req.query);

    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({
      aircraft: AIRCRAFT_PROFILE,
      takeoff: {
        runwayAndWeather: {
          apt: { icao: '', name: '' },
          wind: null,
          rwy: '',
          hwXw: { headwindKt: null, crosswindKt: null },
          toraFt: null,
          oatC: null,
          hdgDeg: null,
          qnhIn: null,
          qnhHpa: null,
          elevFt: null,
          rwyCond: '',
          densityAltFt: null,
          rwySlopePct: null,
          widthFt: null,
          surface: ''
        },
        inputs: {
          weightLbs: AIRCRAFT_PROFILE.maxTakeoffWeightLbs,
          flaps: AIRCRAFT_PROFILE.takeoff.defaultFlaps,
          powerSetting: AIRCRAFT_PROFILE.takeoff.powerSetting,
          ac: AIRCRAFT_PROFILE.takeoff.ac
        },
        outputs: {
          vrKias: null,
          vyKias: null,
          vxKias: null,
          accelCheckFt: null,
          rwyLimitFt: null,
          limitCode: AIRCRAFT_PROFILE.takeoff.limitCode,
          takeoffDistanceRequiredFt: null,
          distance50FtFt: null,
          rotatePitch: AIRCRAFT_PROFILE.takeoff.rotatePitch,
          xwindLimitKt: AIRCRAFT_PROFILE.takeoff.xwindLimitKt
        },
        messages: ['NONE']
      },
      landing: {
        airportAndWeather: {
          apt: { icao: '', name: '' },
          wind: null,
          rwy: '',
          hwXw: { headwindKt: null, crosswindKt: null },
          ldaFt: null,
          oatC: null,
          hdgDeg: null,
          qnhIn: null,
          qnhHpa: null,
          elevFt: null,
          rwyCond: '',
          densityAltFt: null,
          rwySlopePct: null,
          widthFt: null,
          surface: ''
        },
        inputs: {
          landingWeightLbs: AIRCRAFT_PROFILE.maxLandingWeightLbs,
          flaps: AIRCRAFT_PROFILE.landing.defaultFlaps,
          approachSpeedKias: null,
          ac: AIRCRAFT_PROFILE.landing.ac
        },
        outputs: {
          vrefKias: null,
          finalApproachSpeedKias: null,
          landingDistanceRequiredFt: null,
          distance50FtFt: null,
          rwyLimitFt: null,
          limitCode: AIRCRAFT_PROFILE.landing.limitCode,
          braking: AIRCRAFT_PROFILE.landing.braking
        },
        goAround: {
          goAroundPower: AIRCRAFT_PROFILE.landing.goAroundPower,
          flapsUpSpeedKias: null
        }
      }
    });
  }
});

app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  try {
    await loadAirportDatabase();
    await loadRunwayDatabase();
  } catch (err) {
    console.warn('Airport/runway DB warm-up failed:', err.message);
  }
});

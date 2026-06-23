import express from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { randomUUID } from 'crypto';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import { airports as bundledAirports } from './data/airports.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const PILOT_ACCESS_CODE = process.env.PILOT_ACCESS_CODE || 'NATGLOBEOPS';
const PILOT_COOKIE_NAME = 'ng_pilot_access';
const BOOKING_COOKIE_VALUE = 'enabled';
const WALLET_ASSETS_DIR = path.join(__dirname, 'wallet-assets');
const PUBLIC_SITE_URL = String(process.env.PUBLIC_SITE_URL || 'https://ngaprivateaviation.com').replace(/\/$/, '');

// The public domain opens the passenger experience. ACARS remains available to crew at /acars.
app.get('/', (req, res) => {
  res.redirect('/bookings');
});

app.get('/acars', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.use(express.static(path.join(__dirname, 'public')));
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
const FIXED_BOOKING_ROUTE_PRICES = {
  'EFHV-EFHN': { oneWay: 129, roundtrip: 258, label: 'ESTIMATED HYVINKAA-HANKO COST SHARE' },
  'EFHN-EFHV': { oneWay: 129, roundtrip: 258, label: 'ESTIMATED HANKO-HYVINKAA COST SHARE' },
  'EFHK-EFHN': { oneWay: 1500, roundtrip: 3000, label: 'FIXED HELSINKI-HANKO' },
  'EFHN-EFHK': { oneWay: 1500, roundtrip: 3000, label: 'FIXED HANKO-HELSINKI' }
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
    aircraft: 'OH-PMK / Piper PA-28R-200 Arrow II',
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
    aircraft: 'OH-PMK / Piper PA-28R-200 Arrow II',
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
    aircraft: 'OH-PMK / Piper PA-28R-200 Arrow II',
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
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    const [requests, availability, timeline] = await Promise.all([
      bookingPool.query('SELECT payload FROM booking_requests ORDER BY created_at ASC'),
      bookingPool.query('SELECT date, is_open, note, updated_at FROM booking_availability'),
      bookingPool.query('SELECT id, request_id, event_type, note, created_at FROM booking_timeline ORDER BY created_at ASC')
    ]);
    bookingRequests.push(...requests.rows.map(row => row.payload));
    availability.rows.forEach(row => bookingAvailability.set(String(row.date).slice(0, 10), {
      isOpen: row.is_open,
      note: row.note,
      updatedAt: row.updated_at
    }));
    timeline.rows.forEach(row => {
      const items = bookingTimeline.get(row.request_id) || [];
      items.push({ id: row.id, type: row.event_type, note: row.note, createdAt: row.created_at });
      bookingTimeline.set(row.request_id, items);
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

async function addBookingTimelineEvent(requestId, type, note = '') {
  const item = { id: `TML-${randomUUID().slice(0, 8).toUpperCase()}`, type, note, createdAt: new Date().toISOString() };
  const items = bookingTimeline.get(requestId) || [];
  items.push(item);
  bookingTimeline.set(requestId, items);
  if (bookingPool) {
    await bookingPool.query(
      'INSERT INTO booking_timeline (id, request_id, event_type, note, created_at) VALUES ($1, $2, $3, $4, $5)',
      [item.id, requestId, item.type, item.note, item.createdAt]
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
    const prefix = String(request.priceNote || '').includes('ESTIMATED') ? 'Approx. ' : '';
    return `${prefix}EUR ${total} total / EUR ${perPassenger} per passenger`;
  }
  return String(total || perPassenger || 'Price determined by request');
}

function privateFlightEmailHtml({ status, reference, greeting, intro, details, sections = [], closing = [] }) {
  const navy = '#06285d';
  const silver = '#8b9199';
  const paper = '#ffffff';
  const confidentiality = `<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-top:22px;border-collapse:collapse"><tr><td bgcolor="#fff5f5" style="padding:12px 14px;border:1px solid #b42318;color:#8f1712;font-size:11px;line-height:1.55"><strong>CONFIDENTIAL PASSENGER INFORMATION</strong><br>This email and any linked flight information may contain sensitive passenger data. It is intended only for the named recipient and must not be shared, copied, or misused.</td></tr></table>`;
  const detailRows = details.map(([label, value]) => `
    <tr><td bgcolor="${paper}" style="padding:10px 0;border-bottom:1px solid #d5d8dc;width:42%;font-weight:700;color:${navy}">${escapeEmailHtml(label)}</td><td bgcolor="${paper}" style="padding:10px 0;border-bottom:1px solid #d5d8dc;color:#15171a">${escapeEmailHtml(value)}</td></tr>`).join('');
  const sectionHtml = sections.map(section => `
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin:24px 0;border-collapse:collapse"><tr><td bgcolor="${navy}" style="padding:9px 13px;border:1px solid ${navy};color:#ffffff;font-size:13px;font-weight:700;letter-spacing:.08em">${escapeEmailHtml(section.title)}</td></tr><tr><td bgcolor="${paper}" style="padding:10px 0;color:#15171a">${section.html || (section.lines || []).map(line => `<p style="margin:8px 0;line-height:1.6;color:#15171a">${escapeEmailHtml(line)}</p>`).join('')}</td></tr></table>`).join('');
  return `<!doctype html><html><body bgcolor="#ffffff" style="margin:0;padding:0;background-color:#ffffff;font-family:'Courier New',Courier,monospace;color:#15171a">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff;border-collapse:collapse"><tr><td align="center" bgcolor="#ffffff" style="padding:24px 12px;background-color:#ffffff">
      <table role="presentation" width="680" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;max-width:680px;background-color:#ffffff;border:2px solid ${navy};border-collapse:collapse">
        <tr><td bgcolor="#ffffff" style="padding:20px 28px;background-color:#ffffff;border-bottom:2px solid ${navy}">
          <img src="${PUBLIC_SITE_URL}/nga-private-aviation-logo.png" alt="NGA Private Aviation" width="154" style="display:block;width:154px;max-width:100%;height:auto;border:0;margin:0 0 15px" />
          <div style="font-size:12px;letter-spacing:.12em;color:${silver}">NGA PRIVATE AVIATION</div>
          <div style="margin-top:8px;font-size:23px;font-weight:700;letter-spacing:.03em;color:${navy}">${escapeEmailHtml(status)}</div>
          <div style="margin-top:13px;font-size:13px;color:#3f454c">REFERENCE: <strong style="color:${navy}">${escapeEmailHtml(reference)}</strong></div>
        </td></tr>
        <tr><td bgcolor="#ffffff" style="padding:28px;background-color:#ffffff">
          <p style="margin:0 0 18px;font-size:16px;line-height:1.5;color:#15171a">Dear <strong>${escapeEmailHtml(greeting)},</strong></p>
          <p style="margin:0 0 22px;line-height:1.65;color:#15171a">${escapeEmailHtml(intro)}</p>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff;border-collapse:collapse;font-size:14px">${detailRows}</table>
          ${sectionHtml}
          ${closing.map(line => `<p style="margin:6px 0;line-height:1.5;color:#15171a">${escapeEmailHtml(line)}</p>`).join('')}
          ${confidentiality}
        </td></tr>
      </table>
    </td></tr></table>
  </body></html>`;
}

function requestReceivedEmailHtml({ reference, greeting, details }) {
  const navy = '#06285d';
  const silver = '#8b9199';
  const lines = details.map(([label, value]) => `<tr><td bgcolor="#ffffff" style="padding:11px 0;border-bottom:1px solid #d5d8dc;color:${silver};font-size:11px;letter-spacing:.06em;text-transform:uppercase">${escapeEmailHtml(label)}</td><td align="right" bgcolor="#ffffff" style="padding:11px 0;border-bottom:1px solid #d5d8dc;color:${navy};font-size:13px;font-weight:700;line-height:1.35;text-transform:uppercase">${escapeEmailHtml(value)}</td></tr>`).join('');
  return `<!doctype html><html><head><style>@font-face{font-family:'Computer Says No';src:url('${PUBLIC_SITE_URL}/fonts/computer-says-no.woff2') format('woff2');font-weight:700;font-style:normal}</style></head><body bgcolor="#ffffff" style="margin:0;padding:0;background-color:#ffffff;font-family:'Courier New',Courier,monospace;color:#15171a">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff"><tr><td align="center" bgcolor="#ffffff" style="padding:24px 12px;background-color:#ffffff">
      <table role="presentation" width="720" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;max-width:720px;background-color:#ffffff;border:2px solid ${navy};border-collapse:collapse">
        <tr><td bgcolor="#ffffff" style="padding:20px;background-color:#ffffff">
          <img src="${PUBLIC_SITE_URL}/nga-private-aviation-logo.png" alt="NGA Private Aviation" width="154" style="display:block;width:154px;max-width:100%;height:auto;border:0;margin:0 0 15px" />
          <div style="color:${silver};font-size:11px;letter-spacing:.08em;text-transform:uppercase">Private Flight</div>
          <div style="margin:10px 0 14px;color:${navy};font-family:'Computer Says No','Courier New',Courier,monospace;font-size:32px;font-weight:700;line-height:1;text-transform:uppercase">REQUEST RECEIVED</div>
          <p style="margin:0;color:#3f454c;font-size:12px;line-height:1.55;text-transform:uppercase">Your flight request is with the flight team. A pilot will review the route, aircraft, weather, and loading before confirmation.</p>
          <div style="margin:16px 0;padding:12px;border:1px solid ${navy};color:${navy};font-family:'Computer Says No','Courier New',Courier,monospace;font-size:24px;font-weight:700;line-height:1">REFERENCE ${escapeEmailHtml(reference)}</div>
          <p style="margin:0 0 14px;color:#15171a;font-size:14px;line-height:1.5">Dear <strong>${escapeEmailHtml(greeting)},</strong></p>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:100%;background-color:#ffffff;border-collapse:collapse">${lines}</table>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-top:16px;width:100%;border-top:1px solid #d5d8dc;border-collapse:collapse">
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid ${navy};color:${navy};font-size:12px;text-transform:uppercase"><strong>Request received</strong><br><span style="color:#555b63">Your flight details have been received.</span></td></tr>
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid #c5c9ce;color:#15171a;font-size:12px;text-transform:uppercase"><strong>Pilot and operations review</strong><br><span style="color:#555b63">Route, weather, aircraft, and loading are checked.</span></td></tr>
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid #c5c9ce;color:#15171a;font-size:12px;text-transform:uppercase"><strong>Payment instructions after approval</strong><br><span style="color:#555b63">A Reimbursement Statement is issued once confirmed.</span></td></tr>
            <tr><td style="padding:10px 0 10px 12px;border-left:2px solid #c5c9ce;color:#15171a;font-size:12px;text-transform:uppercase"><strong>Final flight confirmation</strong><br><span style="color:#555b63">Passenger passes and final details are sent by email.</span></td></tr>
          </table>
          <div style="margin-top:16px;padding:11px 13px;border:1px solid ${navy};color:#15171a;font-size:12px;line-height:1.55"><strong style="color:${navy}">PAYMENT TERMS</strong><br>Once the flight is confirmed, a Reimbursement Statement will be issued. Payment is due no later than 48 hours before scheduled departure unless operations agrees otherwise. If payment is not received by then, the booking may be cancelled.</div>
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
      label: passenger.name || `Passenger ${passenger.number || 1}`,
      url: `${PUBLIC_SITE_URL}/private-flight-information-pass/${encodeURIComponent(request.id)}/${passenger.boardingPassToken}`
    }));
}

async function notifyBookerOfBooking(request) {
  const recipient = String(request.email || '').trim();
  const details = [
    ['Route', `${boardingPassAirportLabel(request.dep, request.depName)} - ${boardingPassAirportLabel(request.arr, request.arrName)}`],
    ['Date', emailDate(request.requestDate)],
    ['Requested departure', `${formatBoardingPassTime(request.requestTime)} Local Time`],
    ['Estimated flight time', emailFlightTime(request)],
    ['Passengers', request.seats],
    ['Baggage', boardingPassBaggage(request)],
    ['Price estimate', emailPriceSummary(request)]
  ];
  return sendBookingEmail({
    to: recipient,
    subject: `Private flight request received / ${request.id}`,
    text: [
      `Dear ${emailPassengerNames(request)},`,
      '',
      'Thank you for your private flight request. Our operations team has received it and will contact you within 24 hours of the requested flight.',
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
      'Kind regards,',
      'NGA Private Aviation'
    ].join('\n'),
    html: requestReceivedEmailHtml({
      reference: request.id,
      greeting: emailPassengerNames(request),
      details
    })
  });
}

async function notifyBookerOfApproval(request) {
  const recipient = String(request.email || '').trim();
  const passLinks = passengerPassLinks(request);
  const passItems = passengerPassItems(request);
  const agreementUrl = String(process.env.BOOKING_AGREEMENT_URL || '').trim();
  const reimbursementUrl = String(process.env.BOOKING_REIMBURSEMENT_URL || '').trim();
  const details = [
    ['Route', `${boardingPassAirportLabel(request.dep, request.depName)} - ${boardingPassAirportLabel(request.arr, request.arrName)}`],
    ['Date', emailDate(request.requestDate)],
    ['Boarding time', `${boardingPassBoardingTime(request.requestTime)} Local Time`],
    ['Scheduled departure', `${formatBoardingPassTime(request.requestTime)} Local Time`],
    ['Aircraft', request.aircraft || 'OH-PMK / Piper PA-28R-200 Arrow II'],
    ['Total price', emailPriceSummary(request)]
  ];
  const passHtml = passItems.length
    ? passItems.map(item => `<p style="margin:10px 0;color:#f2f2f2"><strong>${escapeEmailHtml(item.label)}</strong><br><a href="${escapeEmailHtml(item.url)}" style="color:#66ff99;font-weight:700;word-break:break-all">OPEN FLIGHT INFORMATION PASS</a></p>`).join('')
    : '<p style="margin:8px 0;line-height:1.55;color:#cfcfcf">Passenger passes will be issued by operations shortly.</p>';
  const documentHtml = [
    agreementUrl ? `<p style="margin:10px 0"><a href="${escapeEmailHtml(agreementUrl)}" style="color:#66ff99;font-weight:700">OPEN PRIVATE FLIGHT AGREEMENT</a></p>` : '<p style="margin:8px 0;color:#cfcfcf">Private Flight Agreement: to be provided by operations.</p>',
    reimbursementUrl ? `<p style="margin:10px 0"><a href="${escapeEmailHtml(reimbursementUrl)}" style="color:#66ff99;font-weight:700">OPEN REIMBURSEMENT STATEMENT</a></p>` : '<p style="margin:8px 0;color:#cfcfcf">Reimbursement Statement: to be provided by operations.</p>'
  ].join('');
  return sendBookingEmail({
    to: recipient,
    subject: `Flight confirmed / ${request.id}`,
    text: [
      `Dear ${emailPassengerNames(request)},`,
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
      'PASSENGER FLIGHT INFORMATION PASSES',
      ...(passLinks.length ? passLinks : ['Passenger passes will be issued by operations shortly.']),
      '',
      'PASSENGER DOCUMENTATION',
      agreementUrl ? `Private Flight Agreement: ${agreementUrl}` : 'Private Flight Agreement: to be provided by operations.',
      reimbursementUrl ? `Reimbursement Statement: ${reimbursementUrl}` : 'Reimbursement Statement: to be provided by operations.',
      '',
      'PAYMENT TERMS',
      'A Reimbursement Statement will be issued for this confirmed flight. Payment is due no later than 48 hours before scheduled departure unless operations agrees otherwise. If payment is not received by then, the booking may be cancelled.',
      '',
      'BAGGAGE AND ITEMS ON BOARD',
      'Personal bags, electronics, medication, and normal personal items may be carried subject to pilot approval. Do not bring weapons, explosives, flammable liquids or gases, dangerous goods, or undeclared lithium batteries.',
      '',
      'Please notify us as soon as possible if your plans or baggage change, or if you expect to arrive late.',
      '',
      'Kind regards,',
      'Antoni Garcia',
      'Pilot in Command',
      'NGA Private Aviation',
      '+358 41 314 5148',
      'antonigarcia200518@icloud.com'
    ].join('\n'),
    html: privateFlightEmailHtml({
      status: 'FLIGHT CONFIRMED',
      reference: request.id,
      greeting: emailPassengerNames(request),
      intro: 'We are pleased to confirm that your private flight request has been approved, subject to normal day-of-flight weather and operational checks.',
      details,
      sections: [
        { title: 'AIRPORT LOCATION', lines: emailAirportLocation(request) },
        { title: 'PASSENGER FLIGHT INFORMATION PASSES', html: passHtml },
        { title: 'PASSENGER DOCUMENTATION', html: documentHtml },
        { title: 'PAYMENT TERMS', lines: ['A Reimbursement Statement will be issued for this confirmed flight. Payment is due no later than 48 hours before scheduled departure unless operations agrees otherwise. If payment is not received by then, the booking may be cancelled.'] },
        { title: 'BAGGAGE AND ITEMS ON BOARD', lines: ['Personal bags, electronics, medication, and normal personal items may be carried subject to pilot approval. Do not bring weapons, explosives, flammable liquids or gases, dangerous goods, or undeclared lithium batteries.'] },
        { title: 'BOARDING REMINDER', lines: ['Please arrive no later than the boarding time and bring a valid form of identification. Keep your mobile phone available for any operational update, and have baggage ready for loading on arrival.'] }
      ],
      closing: ['Kind regards,', 'Antoni Garcia', 'Pilot in Command', 'NGA Private Aviation', '+358 41 314 5148', 'antonigarcia200518@icloud.com']
    })
  });
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
    if (await notifyBookerOfBooking(request)) {
      await addBookingTimelineEvent(request.id, 'PASSENGER EMAIL NOTIFICATION', 'Booking receipt sent to the booking contact.');
    }
  } catch (err) {
    console.error('BOOKER EMAIL:', err.message);
  }
}

async function deliverApprovalNotification(request) {
  try {
    if (await notifyBookerOfApproval(request)) {
      request.confirmationEmailSentAt = new Date().toISOString();
      await persistBookingRequest(request);
      await addBookingTimelineEvent(request.id, 'FLIGHT CONFIRMATION EMAIL', 'Passenger flight confirmation and pass links sent.');
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

const AIRCRAFT_PROFILE = {
  registration: 'OH-PMK',
  type: 'Piper PA-28R-200 Arrow II',
  bestGlideSpeedKt: 91,
  bestGlideRatio: 9,
  defaultCruiseAltitudeFt: 8000,
  maxCeilingFt: 17000,
  basicEmptyWeightLbs: 1727,
  maxTakeoffWeightLbs: 2650,
  maxLandingWeightLbs: 2650,
  maxRampWeightLbs: 2657,
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

function hasPilotAccess(req) {
  const cookies = parseCookies(req);
  return cookies[PILOT_COOKIE_NAME] === BOOKING_COOKIE_VALUE;
}

function requirePilotAccess(req, res, next) {
  if (hasPilotAccess(req)) return next();
  return res.status(401).json({ error: 'PILOT ACCESS REQUIRED' });
}

function pilotCookieOptions() {
  const secure = process.env.NODE_ENV === 'production';
  return [
    `${PILOT_COOKIE_NAME}=${BOOKING_COOKIE_VALUE}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
    'Max-Age=2592000',
    secure ? 'Secure' : ''
  ].filter(Boolean).join('; ');
}

function normalizeBookingText(input, max = 80) {
  return String(input || '')
    .trim()
    .replace(/\s+/g, ' ')
    .slice(0, max);
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

function getRequestPassengers(request) {
  return request.passengers?.length ? request.passengers : [{ number: 1, name: request.name }];
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
  const minutes = Math.max(10, Math.round((kmToNm(haversineKm(depAirport, arrAirport)) / AIRCRAFT_PROFILE.cruiseTasKt) * 60));
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
        { key: 'aircraft', label: 'AIRCRAFT', value: request.aircraft || 'PA-28R-200' }
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
  if (!depAirport || !arrAirport || depAirport.icao === arrAirport.icao) {
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

app.get('/pass-check/:token', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.sendFile(path.join(__dirname, 'views', 'pass-verification.html'));
});

app.post('/api/pilot-login', (req, res) => {
  const code = String(req.body?.code || '').trim();
  if (code !== PILOT_ACCESS_CODE) {
    return res.status(401).json({ error: 'INVALID PILOT CODE' });
  }

  res.setHeader('Set-Cookie', pilotCookieOptions());
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

app.patch('/api/booking-ops/requests/:id', requirePilotAccess, async (req, res) => {
  await bookingStoreReady;
  const request = bookingRequests.find(item => item.id === String(req.params.id || '').trim().toUpperCase());
  if (!request) return res.status(404).json({ error: 'BOOKING REQUEST NOT FOUND' });

  const paymentStatus = String(req.body?.paymentStatus || '').trim().toUpperCase();
  const pilotDecision = String(req.body?.pilotDecision || '').trim().toUpperCase();
  const wasApproved = request.pilotDecision === 'APPROVED' && request.status === 'CONFIRMED';

  if (paymentStatus) {
    const allowedPayments = new Set(['UNPAID', 'DEPOSIT PAID', 'PAID', 'REFUNDED']);
    if (!allowedPayments.has(paymentStatus)) return res.status(400).json({ error: 'INVALID PAYMENT STATUS' });
    request.paymentStatus = paymentStatus;
    await addBookingTimelineEvent(request.id, 'PAYMENT STATUS', paymentStatus);
  }

  if (pilotDecision) {
    const allowedDecisions = new Set(['PENDING', 'APPROVED', 'NEEDS INFO', 'DECLINED']);
    if (!allowedDecisions.has(pilotDecision)) return res.status(400).json({ error: 'INVALID PILOT DECISION' });
    request.pilotDecision = pilotDecision;
    if (pilotDecision === 'APPROVED') request.status = 'CONFIRMED';
    if (pilotDecision === 'NEEDS INFO') request.status = 'NEEDS INFO';
    if (pilotDecision === 'DECLINED') request.status = 'DECLINED';
    if (pilotDecision === 'PENDING') request.status = 'REQUESTED';
    await addBookingTimelineEvent(request.id, 'PILOT DECISION', pilotDecision);
  }

  const timelineNote = normalizeBookingText(req.body?.timelineNote, 300);
  if (timelineNote) await addBookingTimelineEvent(request.id, 'OPS NOTE', timelineNote);
  if (req.body?.crew && typeof req.body.crew === 'object') {
    const allowedCrew = new Set(['ANTONI GARCIA', 'SAUL GARCIA', 'UNASSIGNED', 'NONE']);
    const commander = normalizeBookingText(req.body.crew.commander, 40).toUpperCase();
    const secondary = normalizeBookingText(req.body.crew.secondary, 40).toUpperCase();
    if (!allowedCrew.has(commander) || !allowedCrew.has(secondary) || commander === 'NONE') {
      return res.status(400).json({ error: 'INVALID CREW ASSIGNMENT' });
    }
    request.crew = { commander, secondary };
    await addBookingTimelineEvent(request.id, 'CREW ASSIGNMENT', `CMD ${commander} / SIC ${secondary}`);
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
  if (depAirport.icao === arrAirport.icao) return res.status(400).json({ error: 'CHOOSE DIFFERENT DEPARTURE AND DESTINATION' });

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
    boardingPassToken: createBoardingPassToken()
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
    boardingPassToken: createBoardingPassToken()
  }];

  while (passengers.length < seats) {
    passengers.push({ number: passengers.length + 1, title: '', name: '', email: '', phone: '', dob: '', weightKg: '', passportCountry: '', nationalId: '', boardingPassToken: createBoardingPassToken() });
  }

  const leadPassenger = passengers[0] || {};
  const name = leadPassenger.name;
  const email = leadPassenger.email;
  const phone = leadPassenger.phone;
  const message = normalizeBookingText(req.body?.message, 180);
  const requestDate = normalizeBookingText(req.body?.requestDate, 20);
  const requestTime = normalizeBookingText(req.body?.requestTime, 16);
  const tripType = req.body?.tripType === 'ROUNDTRIP' ? 'ROUNDTRIP' : 'ONE_WAY';
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
  const extrasNotes = normalizeBookingText(req.body?.extrasNotes, 140);
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
  const signatureProcessed = req.body?.signatureProcessed === true || req.body?.signatureProcessed === 'true';

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
  if (!signatureProcessed) {
    return res.status(400).json({ error: 'SAFETY DECLARATION SIGNATURE REQUIRED' });
  }

  const priceEstimate = estimateBookingPrice(depAirport, arrAirport, seats, tripType);

  const request = {
    id: nextBookingReference(depAirport.icao, arrAirport.icao),
    flightId: flight?.id || `NG-RQ-${Date.now().toString().slice(-5)}`,
    flightTitle: flight?.title || `${depAirport.city} to ${arrAirport.city}`,
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
    tripType,
    returnDate,
    returnTime,
    returnPlan,
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

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

/* -------------------- (UNCHANGED CORE LOGIC REMOVED FOR BREVITY — KEEP YOUR ORIGINAL) -------------------- */
/* KEEP EVERYTHING FROM YOUR ORIGINAL FILE EXACTLY THE SAME UNTIL RMK + FORMAT FUNCTIONS */

/* ========================= */
/* 🔥 UPDATED RMK GENERATOR */
/* ========================= */

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

    if (vis !== null) {
      if (vis < 2000) remarks.push('POOR VIS');
      else if (vis < 5000) remarks.push('LOW VIS');
    }

    if (wind) {
      if (wind.gust && wind.gust >= 25) {
        remarks.push(`GUSTS ${wind.gust}KT`);
      } else if (wind.speed >= 20) {
        remarks.push('STRONG WIND');
      }
    }

    if (cloudBase !== null && cloudBase <= 2000) {
      remarks.push('LOW CEILING');
    }

    if (phenomena.includes('TS')) remarks.push('TS');
    else if (phenomena.includes('FG')) remarks.push('FOG');
    else if (phenomena.some(p => ['RA','SN','DZ','SHRA','SHSN','FZRA'].includes(p))) {
      remarks.push('PRECIP');
    }
  }

  inspect(data.depWx);
  inspect(data.arrWx);

  const unique = [...new Set(remarks)];
  return unique.length ? unique.join(' / ') : 'NIL';
}

/* ========================= */
/* 🧾 TEXT REPORT FIX */
/* ========================= */

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

  function pushAirportBlock(label, wx, wantMetar) {
    if (!wx || !wantMetar) return;

    lines.push(`${label} (${wx.airport})`);

    if (wx.metarFallback && wx.metarSource) {
      lines.push('MODE FALLBACK');
      lines.push(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`);
    }

    // ✅ CLEAN: no "METAR" label
    lines.push(wx.metar || 'NOT AVAILABLE');
    lines.push('');
  }

  pushAirportBlock('DEP WX', data.depWx, data.includeDepMetar);
  pushAirportBlock('ARR WX', data.arrWx, data.includeArrMetar);
  pushAirportBlock('ALTN WX', data.altnWx, data.includeAltnMetar);

  lines.push(`RMK ${generateAutoRemark(data)}`);
  lines.push('');
  lines.push('END OF REPORT');

  return lines.join('\n');
}

/* ========================= */
/* 📄 PDF FIX */
/* ========================= */

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

  const drawAirportSection = (title, wx, wantMetar) => {
    if (!wx || !wantMetar) return;

    drawLine(`${title} (${wx.airport})`, 10.5, true);

    if (wx.metarFallback && wx.metarSource) {
      drawLine('MODE FALLBACK', 9.2, true);
      drawLine(`METAR SRC ${wx.metarSource}/${wx.metarDistanceNm}NM`, 9.2, true);
    }

    // ✅ CLEAN: no "METAR" label
    drawWrappedText(wx.metar || 'NOT AVAILABLE');
    y -= 4;
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

  drawAirportSection('DEP WX', data.depWx, data.includeDepMetar);
  drawAirportSection('ARR WX', data.arrWx, data.includeArrMetar);
  drawAirportSection('ALTN WX', data.altnWx, data.includeAltnMetar);

  drawLine(`RMK ${generateAutoRemark(data)}`, 9.5, true);
  y -= 2;

  drawLine('END OF REPORT', 10.5, true);

  return Buffer.from(await pdfDoc.save());
}

/* -------------------- KEEP REST OF YOUR ORIGINAL FILE UNCHANGED -------------------- */

app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  try {
    await loadAirportDatabase();
  } catch (err) {
    console.warn('Airport DB warm-up failed:', err.message);
  }
});

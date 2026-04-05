import express from "express";
import fetch from "node-fetch";
import { PDFDocument, StandardFonts } from "pdf-lib";

const app = express();
app.use(express.json());
app.use(express.static("public"));

const WX_API = "https://aviationweather.gov/api/data/metar?format=raw&ids=";

// ---------------------------
// Fetch METAR
// ---------------------------
async function fetchMetar(icao) {
  try {
    const res = await fetch(`${WX_API}${icao}`, { timeout: 4000 });
    const text = await res.text();
    return text || null;
  } catch {
    return null;
  }
}

// ---------------------------
// Fake fallback (replace later with real nearest search if needed)
// ---------------------------
async function fetchWithFallback(icao) {
  let metar = await fetchMetar(icao);

  if (metar) {
    return { metar, fallback: false };
  }

  // Simple fallback example
  const fallbackIcao = "EHAM";
  const fallbackMetar = await fetchMetar(fallbackIcao);

  return {
    metar: fallbackMetar || "METAR UNAVAILABLE",
    fallback: true,
    source: fallbackIcao,
    distance: 42
  };
}

// ---------------------------
// RMK Generator
// ---------------------------
function generateRMK(metar) {
  if (!metar) return "NIL";

  const rmk = [];

  const gustMatch = metar.match(/G(\d{2,3})KT/);
  if (gustMatch && parseInt(gustMatch[1]) >= 25) {
    rmk.push(`GUSTS ${gustMatch[1]}KT`);
  }

  const visMatch = metar.match(/\s(\d{4})\s/);
  if (visMatch) {
    const vis = parseInt(visMatch[1]);
    if (vis < 2000) rmk.push("POOR VIS");
    else if (vis < 5000) rmk.push("LOW VIS");
  }

  if (/BKN0\d{2}|OVC0\d{2}/.test(metar)) {
    rmk.push("LOW CEILING");
  }

  if (/RA|SN/.test(metar)) rmk.push("PRECIP");
  if (/TS/.test(metar)) rmk.push("TS");
  if (/FG/.test(metar)) rmk.push("FOG");

  return rmk.length ? rmk.join(" / ") : "NIL";
}

// ---------------------------
// Build Report
// ---------------------------
function buildReport(data) {
  const date = new Date();
  const utc = date.toISOString().slice(11, 16);

  return `
ACARS WEATHER REPORT
--------------------

OPS NATGLOBE AVIATION   DATE ${date.toISOString().slice(0, 10)}   UTC ${utc}
FLT ${data.flt}   ORIG ${data.dep}   DEST ${data.arr}   ROUTE ${data.route}

DEP WX (${data.dep})
${data.depFallback ? `MODE FALLBACK\nMETAR SRC ${data.depSrc}/${data.depDist}NM\n` : ""}${data.depMetar}

ARR WX (${data.arr})
${data.arrFallback ? `MODE FALLBACK\nMETAR SRC ${data.arrSrc}/${data.arrDist}NM\n` : ""}${data.arrMetar}

RMK ${data.rmk}
END OF REPORT
`.trim();
}

// ---------------------------
// PDF Generator
// ---------------------------
async function generatePDF(text) {
  const pdfDoc = await PDFDocument.create();
  const page = pdfDoc.addPage([600, 800]);
  const font = await pdfDoc.embedFont(StandardFonts.Courier);

  const lines = text.split("\n");

  let y = 750;

  lines.forEach(line => {
    page.drawText(line.slice(0, 95), {
      x: 40,
      y,
      size: 10,
      font
    });
    y -= 14;
  });

  return await pdfDoc.save();
}

// ---------------------------
// API Route
// ---------------------------
app.post("/generate", async (req, res) => {
  const { flt, dep, arr, route, remarks } = req.body;

  const depData = await fetchWithFallback(dep);
  const arrData = await fetchWithFallback(arr);

  const rmk = remarks || generateRMK(depData.metar + " " + arrData.metar);

  const report = buildReport({
    flt,
    dep,
    arr,
    route,
    depMetar: depData.metar,
    arrMetar: arrData.metar,
    depFallback: depData.fallback,
    arrFallback: arrData.fallback,
    depSrc: depData.source,
    arrSrc: arrData.source,
    depDist: depData.distance,
    arrDist: arrData.distance,
    rmk
  });

  const pdfBytes = await generatePDF(report);

  res.setHeader("Content-Type", "application/pdf");
  res.send(Buffer.from(pdfBytes));
});

// ---------------------------
app.listen(3000, () => {
  console.log("Server running on http://localhost:3000");
});

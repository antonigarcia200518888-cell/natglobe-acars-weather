import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';

// Typesetting only. Weather, category and remark rules remain in the dispatch engine.
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

export async function dispatchToPdfBuffer(data, previewPages, {getMetarFlightCategory, generateAutoRemark, hasAnyWindsLine, buildWindsLine}) {
  const pdfDoc = await PDFDocument.create();
  const regularFont = await pdfDoc.embedFont(StandardFonts.Courier);
  const boldFont = await pdfDoc.embedFont(StandardFonts.CourierBold);

  const pageWidth = 595.28;
  const pageHeight = 841.89;
  let page;
  // Record the same typeset lines used by the PDF, so installed iPad apps can
  // show every page without navigating into a browser-owned PDF viewer.
  const newPage = () => {
    page = pdfDoc.addPage([pageWidth, pageHeight]);
    previewPages?.push([]);
    y = pageHeight - 36;
  };

  const black = rgb(0, 0, 0);
  const blockWidth = 311.81;
  const left = (pageWidth - blockWidth) / 2;
  const indent = 12;
  let y;
  newPage();

  const drawLine = (text, size = 10, bold = false, x = left, color = black, gap = 3.5) => {
    if (y - size < 36) newPage();
    y -= size;
    page.drawText(text, {
      x,
      y,
      size,
      font: bold ? boldFont : regularFont,
      color
    });
    previewPages?.at(-1).push({text,x,y:pageHeight-y,size,bold,ink:[color.red,color.green,color.blue].map(n=>Math.round(n*255))});
    y -= gap;
  };

  const drawWrappedText = (text, size = 9.2, x = left + indent) => {
    const wrapped = wrapText(text, 50);
    for (const line of wrapped) {
      drawLine(line, size, false, x, black, 3);
    }
  };

  const drawWeatherSection = (title, wx, wantMetar, wantTaf) => {
    if (!wx || (!wantMetar && !wantTaf)) return;

    const category = getMetarFlightCategory(wx.metar);
    const categoryColor = category === 'VFR'
      ? rgb(0.05, 0.55, 0.25)
      : category === 'MVFR'
        ? rgb(0.08, 0.32, 0.78)
        : category === 'IFR'
          ? rgb(0.78, 0.08, 0.12)
          : category === 'LIFR'
            ? rgb(0.58, 0.08, 0.68)
            : rgb(0.35, 0.35, 0.35);
    drawLine(`${title} (${wx.airport}) ${category}`, 10.5, true, left, categoryColor);

    if (wantMetar) {
      drawLine('METAR', 9.2, true);
      drawWrappedText(wx.metar || 'NOT AVAILABLE');
      y -= 4;
    }

    if (wantTaf) {
      drawLine('TAF', 9.2, true);
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

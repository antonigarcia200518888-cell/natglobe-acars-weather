const drawLine = (text, size = 18, bold = false) => {
  if (bold) {
    // fake bold (draw twice slightly offset)
    page.drawText(text, {
      x: left,
      y,
      size,
      font: courierFont,
      color: black
    });
    page.drawText(text, {
      x: left + 0.4,
      y,
      size,
      font: courierFont,
      color: black
    });
  } else {
    page.drawText(text, {
      x: left,
      y,
      size,
      font: courierFont,
      color: black
    });
  }

  y -= size + 6;
};

/* OFP and Journey Log share one reader, one saved flight, and a visible exit. */
window.createFlightDocumentReader = function ({ host, baseUrl, save, backToEfb, backToPlan = backToEfb }) {
  const NS = 'http://www.w3.org/2000/svg';
  const pane = document.createElement('section'); pane.className = 'flight-document-reader';
  pane.innerHTML = `<header class="flight-reader-header"><button type="button" data-close>Back to flight plan</button><strong>Flight documents</strong><button type="button" data-efb>Back to EFB</button></header>
    <nav class="flight-document-switcher" aria-label="Select flight document"><button type="button" data-kind="ofp" aria-pressed="true">Operational Flight Plan</button><button type="button" data-kind="journey" aria-pressed="false">Journey Log</button><button type="button" data-kind="both" aria-pressed="false">Side by side</button></nav>
    <div class="flight-document-tools"><button type="button" data-refresh>Regenerate</button><button type="button" data-expand>Expand document</button><button type="button" data-zoom-out aria-label="Zoom out">−</button><output data-zoom>100%</output><button type="button" data-zoom-in aria-label="Zoom in">+</button><button type="button" data-print>Print</button></div>
    <p data-status role="status" aria-live="polite">Choose a document to save and preview the current flight.</p><div class="flight-document-scroll"><div class="flight-document-pages"></div></div>`;
  host.append(pane);
  const dialog = document.createElement('dialog'); dialog.className = 'flight-document-dialog';
  dialog.setAttribute('aria-label', 'Expanded flight documents'); document.body.append(dialog);
  const by = key => pane.querySelector(`[data-${key}]`);
  const pages = pane.querySelector('.flight-document-pages');
  let kind = 'ofp', controller, opener, zoom = 100, urls = [], wasExpanded = false;
  function clean() { urls.forEach(url => URL.revokeObjectURL(url)); urls = []; }
  function expand() {
    if (dialog.open) return;
    opener = document.activeElement; dialog.append(pane); dialog.showModal();
    by('close').focus({ preventScroll: true }); by('expand').textContent = 'Normal view';
  }
  function close() { if (dialog.open) dialog.close(); }
  dialog.addEventListener('close', () => {
    host.append(pane); by('expand').textContent = 'Expand document';
    opener?.focus({ preventScroll: true });
  });
  by('close').onclick = () => dialog.open ? close() : backToPlan();
  by('efb').onclick = () => { close(); backToEfb(); };
  by('expand').onclick = () => dialog.open ? close() : expand();
  const svgElement = (name, attributes) => {
    const node = document.createElementNS(NS, name);
    for (const [key, value] of Object.entries(attributes)) if (value !== undefined) node.setAttribute(key, String(value));
    return node;
  };
  const color = channels => `rgb(${(channels || [20,20,20]).join(',')})`;
  function render(packet) {
    const column = document.createElement('article'); column.className = 'flight-document-column';
    const header = document.createElement('header'), title = document.createElement('h3'), download = document.createElement('a');
    title.textContent = packet.title;
    const bytes = Uint8Array.from(atob(packet.pdf), c => c.charCodeAt(0));
    const url = URL.createObjectURL(new Blob([bytes], { type: 'application/pdf' })); urls.push(url);
    download.href = url; download.download = packet.filename; download.textContent = 'Save PDF';
    header.append(title, download); column.append(header);
    for (const page of packet.pages) {
      const svg = svgElement('svg', { viewBox: `0 0 ${page.width} ${page.height}`, role: 'img', 'aria-label': packet.title + ' page ' + (column.children.length), class: 'flight-document-paper' });
      if (page.background) svg.append(svgElement('image', { href: page.background, x: 0, y: 0, width: page.width, height: page.height }));
      for (const mark of page.marks) {
        let node;
        if (mark.type === 'drawText') {
          node = svgElement('text', { x: mark.x, y: page.height - mark.y, 'font-size': mark.size, 'font-family': mark.mono ? 'NGA Courier, Courier New, monospace' : 'Arial, sans-serif', 'font-weight': mark.bold ? 700 : 400, fill: color(mark.color), transform: mark.angle ? `rotate(${-mark.angle} ${mark.x} ${page.height - mark.y})` : undefined });
          node.textContent = mark.text;
        } else if (mark.type === 'drawLine') {
          node = svgElement('line', { x1: mark.start.x, y1: page.height - mark.start.y, x2: mark.end.x, y2: page.height - mark.end.y, stroke: color(mark.color), 'stroke-width': mark.thickness || 1 });
        } else if (mark.type === 'drawRectangle') {
          node = svgElement('rect', { x: mark.x, y: page.height - mark.y - mark.height, width: mark.width, height: mark.height, fill: mark.fill ? color(mark.color) : 'none', stroke: color(mark.borderColor), 'stroke-width': mark.borderWidth || 0 });
        }
        if (node) svg.append(node);
      }
      column.append(svg);
    }
    return column;
  }
  function setZoom(value) {
    zoom = Math.max(75, Math.min(200, value)); by('zoom').textContent = zoom + '%';
    pages.style.setProperty('--document-zoom', zoom / 100);
  }
  async function load(next = kind) {
    kind = next;
    controller?.abort(); const current = new AbortController(); controller = current;
    clean(); pages.replaceChildren(); pages.dataset.layout = kind;
    by('status').textContent = 'Saving current edits and generating flight documents…';
    pane.querySelectorAll('[data-kind]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.kind === kind)));
    const timeout = setTimeout(() => current.abort(), 30000);
    try {
      await save();
      if (controller !== current) return;
      if (current.signal.aborted) throw Error('Document request timed out.');
      const types = kind === 'both' ? ['ofp', 'journey'] : [kind];
      const results = await Promise.all(types.map(async type => {
        const response = await fetch(baseUrl + (type === 'journey' ? '/journey-log/pdf' : '/operational-flight-plan/pdf'), { signal: current.signal, cache: 'no-store', headers: { Accept: 'application/json' } });
        const packet = await response.json();
        if (!response.ok) throw Error(packet.error || 'Document unavailable');
        if (!Array.isArray(packet.pages) || !packet.pdf) throw Error('Document response is incomplete.');
        return packet;
      }));
      if (controller !== current || current.signal.aborted) return;
      results.forEach(packet => pages.append(render(packet)));
      by('status').textContent = 'Saved flight snapshot · Generated ' + new Date().toISOString().slice(11,19) + ' UTC · Regenerate after editing.';
    } catch (error) {
      if (controller !== current) return;
      by('status').textContent = current.signal.aborted ? 'Document request timed out. Check the connection and tap Regenerate.' : error.message;
    } finally { clearTimeout(timeout); }
  }
  pane.querySelectorAll('[data-kind]').forEach(button => { button.onclick = () => load(button.dataset.kind); });
  by('refresh').onclick = () => load();
  by('zoom-in').onclick = () => setZoom(zoom + 25);
  by('zoom-out').onclick = () => setZoom(zoom - 25);
  by('print').onclick = async () => {
    if (!pages.children.length) { by('status').textContent = 'Generate a document before printing.'; return; }
    wasExpanded = dialog.open; expand(); await document.fonts.ready; window.print();
  };
  window.addEventListener('afterprint', () => { if (!wasExpanded) close(); });
  window.addEventListener('pagehide', event => { if (!event.persisted) { controller?.abort(); clean(); } });
  return { open(type = 'ofp', full = false) { if (full) expand(); return load(type); }, expand, close };
};

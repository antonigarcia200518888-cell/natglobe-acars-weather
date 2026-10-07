/* Standalone design preview. No pilot API calls, geolocation requests or release actions. */
(() => {
  'use strict';

  // Bundled coordinates from data/airports.js. A direct line is not a validated route.
  const airports = Object.freeze({
    EFHV: { name:'Hyvinkaa', lat:60.6544, lon:24.8811 },
    EFTP: { name:'Tampere-Pirkkala', lat:61.4141, lon:23.6044 },
    EFHK: { name:'Helsinki-Vantaa', lat:60.3172, lon:24.9633 }
  });
  const radians = degrees => degrees * Math.PI / 180;
  function calculatePlan({ departure, destination, speed, altitude, rules }) {
    if (!Object.hasOwn(airports, departure) || !Object.hasOwn(airports, destination)) throw new Error('Choose airports from the sample list.');
    if (departure === destination) throw new Error('Choose different departure and destination airports.');
    if (!Number.isFinite(speed) || speed < 30 || speed > 600) throw new Error('Enter a planning speed from 30 to 600 KT.');
    if (!Number.isFinite(altitude) || altitude < 0 || altitude > 45000) throw new Error('Enter a sample altitude from 0 to 45,000 FT.');
    if (!['VFR','IFR'].includes(rules)) throw new Error('Select VFR or IFR.');
    const a = airports[departure], b = airports[destination];
    const h = Math.sin(radians(b.lat - a.lat) / 2) ** 2
      + Math.cos(radians(a.lat)) * Math.cos(radians(b.lat)) * Math.sin(radians(b.lon - a.lon) / 2) ** 2;
    const distance = 3440.065 * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(Math.max(0, 1 - h)));
    return { departure, destination, speed, altitude, rules, distance, minutes:distance / speed * 60 };
  }

  // Keep endpoints clear of the floating sheet, toolbar, dock and zoom controls.
  function routePadding({ width, height, layout, sheetWidth = 0, sheetHeight = 0 }) {
    const compact = width <= 680;
    const split = layout === 'split';
    const top = Math.min(compact ? 100 : 112, height * .25);
    const bottom = Math.min(144 + (compact && split ? sheetHeight : 0), Math.max(0, height - top - 100));
    const left = !compact && split ? sheetWidth + 54 : 54;
    return { paddingTopLeft:[Math.min(left, width * .55), top], paddingBottomRight:[76, bottom] };
  }

  // Pure model is exposed for lightweight regression tests and later adaptation.
  window.efbPreviewModel = Object.freeze({ calculatePlan, routePadding, airports });
  const element = id => document.getElementById(id);
  const all = selector => [...document.querySelectorAll(selector)];
  const preferenceKey = 'nga-efb-design-preview-v1';
  let preferences = {};
  try { preferences = JSON.parse(localStorage.getItem(preferenceKey) || '{}') || {}; } catch (_) { /* Storage is optional. */ }
  let theme = preferences.theme === 'day' ? 'day' : 'night';
  let layout = preferences.layout === 'map' ? 'map' : 'split';
  let material = preferences.material === 'solid' ? 'solid' : 'glass';
  let draftRules = 'VFR';
  let plan = calculatePlan({ departure:'EFHV', destination:'EFTP', speed:110, altitude:4500, rules:'VFR' });
  let map, basemap, routeLine, routeHalo, airportMarkers = [];
  let activeView = '';
  let mapPanel = 'map';
  let routeNeedsFit = false;
  let clockTimer;
  const titles = { maps:'Maps', plates:'Plates', documents:'Documents', 'flight-plan':'Flight plan', settings:'Settings' };

  function rememberPreferences() {
    try { localStorage.setItem(preferenceKey, JSON.stringify({theme,layout,material})); } catch (_) { /* Private mode still supports all controls. */ }
  }
  function pressed(selector, attribute, value) {
    all(selector).forEach(button => button.setAttribute('aria-pressed', String(button.dataset[attribute] === value)));
  }
  function routeColor() { return theme === 'night' ? '#79D7E4' : '#0C6170'; }
  function applyMaterial(value) {
    material = value === 'solid' ? 'solid' : 'glass';
    document.documentElement.dataset.material = material;
    pressed('[data-material-choice]', 'materialChoice', material);
    rememberPreferences();
  }
  function applyTheme(value) {
    theme = value === 'day' ? 'day' : 'night';
    document.documentElement.dataset.theme = theme;
    document.querySelector('meta[name="theme-color"]').content = theme === 'night' ? '#12161A' : '#F5F7FA';
    pressed('[data-theme-choice]', 'themeChoice', theme);
    routeLine?.setStyle({ color:routeColor() });
    airportMarkers.forEach(marker => marker.setStyle({ color:routeColor(), fillColor:theme === 'night' ? '#12161A' : '#FFFFFF' }));
    rememberPreferences();
  }
  function applyLayout(value) {
    layout = value === 'map' ? 'map' : 'split';
    element('panel-maps').dataset.layout = layout;
    element('expandMap').setAttribute('aria-pressed', String(layout === 'map'));
    element('expandMap').setAttribute('aria-label', layout === 'map' ? 'Show flight panel' : 'Expand map workspace');
    pressed('[data-layout-choice]', 'layoutChoice', layout);
    rememberPreferences();
    requestAnimationFrame(() => { map?.invalidateSize({pan:false}); fitRoute(); });
  }
  function showView(value, updateHistory = true) {
    const view = Object.hasOwn(titles, value) ? value : 'maps';
    activeView = view;
    document.documentElement.dataset.view = view;
    all('.view').forEach(panel => { panel.hidden = panel.id !== `panel-${view}`; });
    all('[data-view]').forEach(button => {
      const selected = button.dataset.view === view;
      button.setAttribute('aria-selected', String(selected));
      button.tabIndex = selected ? 0 : -1;
    });
    element('workspaceTitle').textContent = titles[view];
    document.title = `${titles[view]} · NGA design preview`;
    if (updateHistory && location.hash !== `#${view}`) history.pushState(null, '', `#${view}`);
    if (view === 'maps') requestAnimationFrame(() => {
      if (!map) initializeMap();
      else { map.invalidateSize({pan:false}); if (routeNeedsFit) fitRoute(); }
    });
  }
  function setMapPanel(value) {
    mapPanel = value === 'checklist' ? 'checklist' : 'map';
    element('panel-maps').dataset.mapPanel = mapPanel;
    element('mapPanel').hidden = mapPanel !== 'map';
    element('checklistPanel').hidden = mapPanel !== 'checklist';
    element('fitRoute').disabled = mapPanel !== 'map' || !map;
    pressed('[data-map-panel]', 'mapPanel', mapPanel);
    if (mapPanel === 'map') requestAnimationFrame(() => { map?.invalidateSize({pan:false}); if (routeNeedsFit) fitRoute(); });
  }
  function renderPlan() {
    const values = {
      departure:plan.departure, destination:plan.destination,
      'departure-name':airports[plan.departure].name, 'destination-name':airports[plan.destination].name,
      route:`${plan.departure} DCT ${plan.destination}`, distance:plan.distance.toFixed(1),
      eet:Math.round(plan.minutes), altitude:plan.altitude.toLocaleString('en-GB'), speed:plan.speed, rules:plan.rules
    };
    Object.entries(values).forEach(([key,value]) => all(`[data-${key}]`).forEach(node => { node.textContent = value; }));
    if (map) drawRoute();
  }
  function routeCoordinates() {
    return [plan.departure,plan.destination].map(icao => [airports[icao].lat, airports[icao].lon]);
  }
  function fitRoute() {
    if (!map) return;
    if (activeView !== 'maps' || mapPanel !== 'map') { routeNeedsFit = true; return; }
    routeNeedsFit = false;
    const canvas = element('flightMap').getBoundingClientRect();
    const sheet = document.querySelector('.flight-pane').getBoundingClientRect();
    const padding = routePadding({width:canvas.width,height:canvas.height,layout,sheetWidth:sheet.width,sheetHeight:sheet.height});
    map.fitBounds(routeCoordinates(), {...padding,maxZoom:10,animate:false});
  }
  function drawRoute() {
    routeLine?.remove();
    routeHalo?.remove();
    airportMarkers.forEach(marker => marker.remove());
    const coordinates = routeCoordinates();
    routeHalo = window.L.polyline(coordinates, { color:'#12161A', weight:7, opacity:.75, interactive:false }).addTo(map);
    routeLine = window.L.polyline(coordinates, { color:routeColor(), weight:3, dashArray:'8 6', interactive:false }).addTo(map);
    airportMarkers = [plan.departure,plan.destination].map((icao,index) => window.L.circleMarker(coordinates[index], {
      color:routeColor(), fillColor:theme === 'night' ? '#12161A' : '#FFFFFF', fillOpacity:1, weight:3, radius:7
    }).addTo(map).bindTooltip(icao, {permanent:true,direction:index === 0 ? 'right' : 'left',offset:[index === 0 ? 12 : -12,0],opacity:1}));
    fitRoute();
  }
  function setMapStatus(message, error = false) {
    element('mapStatus').textContent = message;
    element('mapStatus').parentElement.dataset.state = error ? 'error' : 'available';
    element('retryMap').hidden = !error || !basemap;
  }
  function initializeMap() {
    if (!window.L) {
      setMapStatus('Map library unavailable. Reload when the app assets are available.', true);
      element('fitRoute').disabled = true;
      return;
    }
    map = window.L.map('flightMap', {zoomControl:false, attributionControl:true, minZoom:5, maxZoom:13, zoomAnimation:false, fadeAnimation:false});
    map.attributionControl.setPrefix(false);
    window.L.control.zoom({position:'bottomright'}).addTo(map);
    // Visible tiles only. Browser HTTP caching is retained; no offline/prefetch worker.
    basemap = window.L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom:19, keepBuffer:1, updateWhenIdle:true,
      attribution:'© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a> · <a href="https://www.openstreetmap.org/fixthemap" target="_blank" rel="noopener">Report map issue</a>'
    });
    let failedTiles = 0;
    basemap.on('loading', () => { failedTiles = 0; setMapStatus('Loading geographic basemap…'); });
    basemap.on('tileerror', () => { failedTiles += 1; setMapStatus('Basemap incomplete · some tiles unavailable.', true); });
    basemap.on('load', () => setMapStatus(failedTiles ? 'Basemap incomplete · retry when connected.' : 'OpenStreetMap · geographic reference only', failedTiles > 0));
    basemap.addTo(map);
    drawRoute();
    element('fitRoute').disabled = mapPanel !== 'map';
    // A single observer coalesces rotation, pane changes and viewport resizing.
    let resizeFrame;
    new ResizeObserver(() => {
      cancelAnimationFrame(resizeFrame);
      resizeFrame = requestAnimationFrame(() => {
        if (activeView === 'maps' && mapPanel === 'map') { map.invalidateSize({pan:false}); fitRoute(); }
      });
    }).observe(element('flightMap'));
  }
  function updateClock() {
    const now = new Date();
    const label = now.toISOString().slice(11,19);
    const z = document.createElement('span');
    z.textContent = 'Z';
    element('zuluTime').replaceChildren(document.createTextNode(label), z);
    element('zuluTime').dateTime = now.toISOString();
  }
  function runClock() {
    clearInterval(clockTimer);
    updateClock();
    if (!document.hidden) clockTimer = setInterval(updateClock, 1000);
  }
  function updateConnection() {
    const online = navigator.onLine;
    element('connectionStatus').dataset.state = online ? 'online' : 'offline';
    element('connectionStatus').querySelector('span').textContent = online ? 'Online' : 'Offline';
  }

  all('[data-view]').forEach(button => button.addEventListener('click', () => showView(button.dataset.view)));
  document.querySelector('.skip-link').addEventListener('click', event => {
    event.preventDefault();
    element('main-content').focus({preventScroll:true});
  });
  all('[data-view-link]').forEach(button => button.addEventListener('click', () => {
    showView(button.dataset.viewLink);
    element('workspaceTitle').focus({preventScroll:true});
  }));
  all('[data-theme-choice]').forEach(button => button.addEventListener('click', () => applyTheme(button.dataset.themeChoice)));
  all('[data-material-choice]').forEach(button => button.addEventListener('click', () => applyMaterial(button.dataset.materialChoice)));
  all('[data-layout-choice]').forEach(button => button.addEventListener('click', () => applyLayout(button.dataset.layoutChoice)));
  all('[data-map-panel]').forEach(button => button.addEventListener('click', () => setMapPanel(button.dataset.mapPanel)));
  all('[data-flight-rules]').forEach(button => button.addEventListener('click', () => {
    draftRules = button.dataset.flightRules;
    pressed('[data-flight-rules]', 'flightRules', draftRules);
    element('planFeedback').dataset.state = 'pending';
    element('planFeedback').textContent = 'Sample changes pending. Update the route to apply.';
  }));
  element('samplePlanForm').addEventListener('input', () => {
    element('planFeedback').dataset.state = 'pending';
    element('planFeedback').textContent = 'Sample changes pending. Update the route to apply.';
  });
  all('[data-preview-check]').forEach(checkbox => checkbox.addEventListener('change', () => {
    const count = all('[data-preview-check]:checked').length;
    element('checklistCount').textContent = `${count} / 4`;
    element('checklistCount').className = `badge ${count === 4 ? 'normal' : 'neutral'}`;
  }));
  document.querySelector('.bottom-nav').addEventListener('keydown', event => {
    if (!['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) return;
    const tabs = all('[data-view]');
    const index = tabs.indexOf(event.target.closest('[data-view]'));
    if (index < 0) return;
    event.preventDefault();
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    showView(tabs[next].dataset.view);
    tabs[next].focus();
  });
  element('samplePlanForm').addEventListener('submit', event => {
    event.preventDefault();
    const feedback = element('planFeedback');
    try {
      plan = calculatePlan({departure:element('sampleDeparture').value,destination:element('sampleDestination').value,speed:Number(element('sampleSpeed').value),altitude:Number(element('sampleAltitude').value),rules:draftRules});
      renderPlan();
      feedback.dataset.state = 'updated';
      feedback.textContent = 'Sample route updated. No operational record was changed.';
    } catch (error) { feedback.dataset.state = 'error'; feedback.textContent = error.message; }
  });
  element('fitRoute').addEventListener('click', fitRoute);
  element('expandMap').addEventListener('click', () => applyLayout(layout === 'split' ? 'map' : 'split'));
  element('retryMap').addEventListener('click', () => basemap?.redraw());
  window.addEventListener('hashchange', () => showView(location.hash.slice(1), false));
  window.addEventListener('online', updateConnection);
  window.addEventListener('offline', updateConnection);
  document.addEventListener('visibilitychange', runClock);
  if (typeof navigator.getBattery === 'function') {
    navigator.getBattery().then(battery => {
      const update = () => {
        element('batteryLevel').textContent = Number.isFinite(battery.level) ? `${Math.round(battery.level * 100)}%` : 'N/A';
        element('batteryDetail').textContent = battery.charging ? 'Charging' : 'On battery';
      };
      update();
      battery.addEventListener('levelchange', update);
      battery.addEventListener('chargingchange', update);
    }).catch(() => { /* Keep the explicit N/A / Not reported state. */ });
  }
  applyTheme(theme);
  applyMaterial(material);
  applyLayout(layout);
  renderPlan();
  updateConnection();
  runClock();
  showView(location.hash.slice(1), false);
})();

/* Isolated map adapter: flight state stays in Pilot Ops; MapLibre owns rendering. */
(function (root) {
  'use strict';
  const VERSION = '5.24.0';
  const STYLES = { day: 'https://tiles.openfreemap.org/styles/liberty', night: 'https://tiles.openfreemap.org/styles/dark' };
  let enginePromise;

  function coordinates(value) {
    if (!value || typeof value.lon !== 'number' || typeof value.lat !== 'number') return null;
    return Number.isFinite(value.lon) && Number.isFinite(value.lat) && Math.abs(value.lon) <= 180 && Math.abs(value.lat) <= 90
      ? [value.lon, value.lat] : null;
  }

  // Shortest great-circle path, with unwrapped longitude across the antimeridian.
  function routeCoordinates(from, to) {
    const start = coordinates(from), end = coordinates(to);
    if (!start || !end) return [];
    const rad = Math.PI / 180;
    const vector = ([lon, lat]) => [Math.cos(lat * rad) * Math.cos(lon * rad), Math.cos(lat * rad) * Math.sin(lon * rad), Math.sin(lat * rad)];
    const a = vector(start), b = vector(end);
    const angle = Math.acos(Math.max(-1, Math.min(1, a.reduce((sum, n, i) => sum + n * b[i], 0))));
    // Antipodal points have no unique shortest path; don't imply a valid route.
    if (Math.PI - angle < 1e-5) return [];
    let previous = start[0];
    return Array.from({ length: 65 }, (_, i) => {
      const t = i / 64;
      const x = angle < 1e-8 ? a : a.map((n, j) => (n * Math.sin((1 - t) * angle) + b[j] * Math.sin(t * angle)) / Math.sin(angle));
      let lon = Math.atan2(x[1], x[0]) / rad;
      while (lon - previous > 180) lon -= 360;
      while (lon - previous < -180) lon += 360;
      previous = lon;
      return [lon, Math.atan2(x[2], Math.hypot(x[0], x[1])) / rad];
    });
  }

  function flightGeometry(flight) {
    const path = routeCoordinates(flight?.departure, flight?.arrival);
    const features = [];
    if (path.length) features.push({ type: 'Feature', properties: {}, geometry: { type: 'LineString', coordinates: path } });
    for (const [key, label] of [['departure', flight?.dep], ['arrival', flight?.arr]]) {
      const point = coordinates(flight?.[key]);
      if (point) features.push({ type: 'Feature', properties: { label: String(label || '').slice(0, 12), kind: key }, geometry: { type: 'Point', coordinates: point } });
    }
    return { type: 'FeatureCollection', features };
  }

  function loadEngine() {
    if (root.maplibregl) return Promise.resolve(root.maplibregl);
    if (!enginePromise) enginePromise = new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.src = `/vendor/maplibre-gl/maplibre-gl-csp.js?v=${VERSION}`;
      script.async = true;
      script.onload = () => root.maplibregl ? resolve(root.maplibregl) : reject(new Error('Map engine unavailable'));
      script.onerror = () => { script.remove(); enginePromise = null; reject(new Error('Map engine unavailable')); };
      document.head.append(script);
    });
    return enginePromise;
  }

  function createPilotMapView({ container, fallback, status, onState = () => {} }) {
    let map, ready = false, active = false, destroyed = false, starting = false, failed = false;
    let flight = null, flightKey = '', theme = document.documentElement.dataset.efbTheme === 'day' ? 'day' : 'night';
    let mode = 'globe', rotating = false, frame = 0, previousTime = 0, timer = 0, styleFrame = 0;
    let currentStyle = theme, fitPending = true, styleReady = false, attempt = 0;
    const reducedMotion = root.matchMedia('(prefers-reduced-motion: reduce)');
    const announce = () => onState({ ready, mode, rotating, hasRoute: routeCoordinates(flight?.departure, flight?.arrival).length > 0 });
    const message = (text, state) => { status.textContent = text; status.dataset.state = state; };

    function stopAnimation() {
      root.cancelAnimationFrame(frame); frame = 0; previousTime = 0;
    }
    function animate(time) {
      frame = 0;
      if (!ready || !active || document.hidden || !rotating || mode !== 'globe' || reducedMotion.matches) return;
      if (previousTime && !map.isMoving()) {
        const center = map.getCenter();
        map.jumpTo({ center: [center.lng + Math.min(time - previousTime, 50) * .002, center.lat] });
      }
      previousTime = time;
      frame = root.requestAnimationFrame(animate);
    }
    function syncAnimation() {
      stopAnimation();
      if (ready && active && rotating && !document.hidden && mode === 'globe' && !reducedMotion.matches) frame = root.requestAnimationFrame(animate);
    }
    function setRotating(value) {
      rotating = Boolean(value) && mode === 'globe' && !reducedMotion.matches;
      syncAnimation(); announce();
    }
    function fitRoute() {
      if (!ready || !active) { fitPending = true; return; }
      fitPending = false;
      setRotating(false);
      const path = routeCoordinates(flight?.departure, flight?.arrival);
      const duration = reducedMotion.matches ? 0 : 650;
      if (path.length) {
        const bounds = path.reduce((box, point) => box.extend(point), new root.maplibregl.LngLatBounds(path[0], path[0]));
        map.fitBounds(bounds, { padding: 64, maxZoom: 8, duration, bearing: 0, pitch: 0 });
      } else map.easeTo({ center: [24, 45], zoom: mode === 'globe' ? .8 : 3.5, bearing: 0, pitch: 0, duration });
    }
    function drawFlight() {
      if (!map || !styleReady) return;
      const data = flightGeometry(flight);
      const source = map.getSource('pilot-route');
      if (source) source.setData(data);
      else {
        map.addSource('pilot-route', { type: 'geojson', data });
        map.addLayer({ id: 'pilot-route-casing', type: 'line', source: 'pilot-route', filter: ['==', '$type', 'LineString'], paint: { 'line-color': '#102d44', 'line-width': 7, 'line-opacity': .8 } });
        map.addLayer({ id: 'pilot-route-line', type: 'line', source: 'pilot-route', filter: ['==', '$type', 'LineString'], paint: { 'line-color': '#60d6ff', 'line-width': 3 } });
        map.addLayer({ id: 'pilot-route-airports', type: 'circle', source: 'pilot-route', filter: ['==', '$type', 'Point'], paint: { 'circle-radius': 7, 'circle-color': ['match', ['get', 'kind'], 'arrival', '#70e0b2', '#ffffff'], 'circle-stroke-width': 3, 'circle-stroke-color': '#16384a' } });
        map.addLayer({ id: 'pilot-route-labels', type: 'symbol', source: 'pilot-route', filter: ['==', '$type', 'Point'], layout: { 'text-field': ['get', 'label'], 'text-font': ['Noto Sans Regular'], 'text-size': 15, 'text-offset': [0, -1.5] }, paint: { 'text-color': '#ffffff', 'text-halo-color': '#132c3e', 'text-halo-width': 2 } });
      }
    }
    function fail() {
      if (destroyed) return;
      attempt++;
      failed = true; ready = false; starting = false; fitPending = true; styleReady = false;
      root.clearTimeout(timer); stopAnimation();
      map?.remove(); map = null;
      container.hidden = true; fallback.hidden = false;
      message('Vector map unavailable · simplified globe shown', 'limited'); announce();
    }
    async function start() {
      if (map || starting || destroyed || failed) return;
      const generation = ++attempt;
      starting = true;
      message('Loading vector map…', 'loading');
      timer = root.setTimeout(fail, 20000);
      try {
        const engine = await loadEngine();
        if (destroyed || failed || generation !== attempt) return;
        engine.setWorkerUrl(`/vendor/maplibre-gl/maplibre-gl-csp-worker.js?v=${VERSION}`);
        container.hidden = false;
        currentStyle = theme;
        map = new engine.Map({ container, style: STYLES[theme], center: [24, 45], zoom: .8, maxZoom: 14, attributionControl: false, pixelRatio: Math.min(root.devicePixelRatio || 1, 2), renderWorldCopies: false });
        map.addControl(new engine.NavigationControl({ visualizePitch: true }), 'top-right');
        map.addControl(new engine.AttributionControl({ compact: true }), 'bottom-right');
        map.on('style.load', () => {
          styleReady = true;
          if (ready) root.clearTimeout(timer);
          if (currentStyle === 'night') {
            // Brighter coastline/labels than the stock dark basemap, without a CSS filter.
            for (const layer of map.getStyle().layers) {
              if (layer.id === 'background') map.setPaintProperty(layer.id, 'background-color', '#233e4b');
              if (layer.id === 'water') map.setPaintProperty(layer.id, 'fill-color', '#102535');
              if (layer.id === 'landcover_glacier' || layer.id === 'landcover_ice_shelf') map.setPaintProperty(layer.id, 'fill-color', '#496373');
              if (layer.id.startsWith('boundary_country')) map.setPaintProperty(layer.id, 'line-color', '#5a7a8a');
              if (layer.type === 'symbol' && layer.id.startsWith('place_')) {
                map.setPaintProperty(layer.id, 'text-color', '#d2e3ee');
                map.setPaintProperty(layer.id, 'text-halo-color', '#19303f');
              }
            }
          }
          map.setProjection({ type: mode === 'globe' ? 'globe' : 'mercator' });
          // style.load permits adding sources even while source tiles are loading.
          drawFlight();
        });
        map.on('load', () => {
          root.clearTimeout(timer); ready = true; starting = false;
          fallback.hidden = true;
          message('Vector basemap · online', 'ready');
          map.resize();
          if (fitPending) fitRoute();
          setTheme(theme); syncAnimation(); announce();
        });
        map.on('error', () => {
          if (ready) message('Some map detail is unavailable · check connection', 'limited');
        });
        map.on('idle', () => { if (ready && navigator.onLine && status.dataset.state !== 'limited') message('Vector basemap · online', 'ready'); });
        map.on('webglcontextlost', fail);
        for (const name of ['dragstart', 'zoomstart', 'rotatestart', 'pitchstart']) map.on(name, event => { if (event.originalEvent) setRotating(false); });
      } catch { if (generation === attempt) fail(); }
    }
    function setActive(value) {
      active = Boolean(value);
      if (active) {
        start();
        root.requestAnimationFrame(() => { if (map && active) { map.resize(); if (fitPending) fitRoute(); } });
      }
      syncAnimation();
    }
    function setFlight(value) {
      const nextKey = JSON.stringify([value?.dep, value?.arr, coordinates(value?.departure), coordinates(value?.arrival)]);
      flight = value;
      if (nextKey === flightKey) return;
      flightKey = nextKey; fitPending = true;
      drawFlight(); if (ready) fitRoute(); announce();
    }
    function setTheme(value) {
      theme = value === 'day' ? 'day' : 'night';
      // Coalesce rapid toggles; changing style does not recreate the map/camera.
      root.cancelAnimationFrame(styleFrame);
      styleFrame = root.requestAnimationFrame(() => {
        if (map && ready && currentStyle !== theme) {
          currentStyle = theme; styleReady = false;
          root.clearTimeout(timer); timer = root.setTimeout(fail, 20000);
          message('Updating map appearance…', 'loading');
          map.setStyle(STYLES[theme]);
        }
      });
    }
    function setMode(value) {
      if (!ready) return;
      mode = value === 'map' ? 'map' : 'globe';
      setRotating(false);
      map.setProjection({ type: mode === 'globe' ? 'globe' : 'mercator' });
      // Keep the flight and camera. Zooming out reveals the spherical projection.
      if (mode === 'globe') map.easeTo({ zoom: Math.min(map.getZoom(), .8), pitch: 0, duration: reducedMotion.matches ? 0 : 650 });
      announce();
    }
    const resize = new ResizeObserver(() => { if (active && map) map.resize(); });
    resize.observe(container);
    const motionChanged = () => { if (reducedMotion.matches) setRotating(false); };
    reducedMotion.addEventListener('change', motionChanged);
    const connectionChanged = () => {
      if (!navigator.onLine) message('Offline · basemap detail may be incomplete', 'limited');
      else if (failed) { failed = false; if (active) start(); }
    };
    root.addEventListener('online', connectionChanged); root.addEventListener('offline', connectionChanged);
    return {
      get ready() { return ready; }, setActive, setFlight, setTheme, setMode, setRotating, fitRoute,
      destroy() { destroyed = true; stopAnimation(); root.clearTimeout(timer); root.cancelAnimationFrame(styleFrame); resize.disconnect(); reducedMotion.removeEventListener('change', motionChanged); root.removeEventListener('online', connectionChanged); root.removeEventListener('offline', connectionChanged); map?.remove(); }
    };
  }
  root.PilotMapGeometry = Object.freeze({ coordinates, routeCoordinates, flightGeometry });
  root.createPilotMapView = createPilotMapView;
})(window);

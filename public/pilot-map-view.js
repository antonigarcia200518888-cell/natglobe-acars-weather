/* Isolated map adapter: flight state stays in Pilot Ops; MapLibre owns rendering. */
(function (root) {
  'use strict';
  const VERSION = '5.24.0';
  const STYLES = { day: 'https://tiles.openfreemap.org/styles/liberty', night: 'https://tiles.openfreemap.org/styles/dark' };
  // Regional overview, never an inferred aircraft position.
  const HOME = { center:[24.8811,60.6544], zoom:6.5 };
  const DAY_LAND = Object.freeze({water:'#b2cbdc',landuse_residential:'#e6e5e8',landcover_wood:'#cad6cf',landcover_grass:'#dee4dc',park:'#d8e1d8',aeroway_fill:'#c7c8d0'});
  const pref = key => { try { return root.localStorage?.getItem('nga-map-'+key); } catch { return null; } };
  const save = (key,value) => { try { root.localStorage?.setItem('nga-map-'+key,value); } catch {} };
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
    const path = flight?.showRoute === false ? [] : routeCoordinates(flight?.departure, flight?.arrival);
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

  function createPilotMapView({ container, fallback, status, onState = () => {}, getPadding = () => 64, startupGlobe = false, onAirport = () => {} }) {
    let map, ready = false, active = false, destroyed = false, starting = false, failed = false;
    let flight = null, flightKey = '', theme = document.documentElement.dataset.efbTheme === 'day' ? 'day' : 'night';
    let mode = startupGlobe || pref('projection') === 'globe' ? 'globe' : 'map', chartCamera = null;
    let startupCamera=null;try{const c=JSON.parse(pref('startup'));if(c&&coordinates({lon:c.center?.[0],lat:c.center?.[1]})&&Number.isFinite(c.zoom)&&c.zoom>=0&&c.zoom<=14)startupCamera=c;}catch{}
    let favorites=[];try{const f=JSON.parse(pref('favorites'));if(Array.isArray(f))favorites=f.filter(v=>/^[A-Z]{4}$/.test(v)).slice(0,50);}catch{}
    let terrain=Number(pref('terrain') ?? 1),terrainFailed=false,recorded={type:'FeatureCollection',features:[]},replayMarker=null;
    if(![0,1,1.5].includes(terrain))terrain=1;
    let rotating = false, frame = 0, previousTime = 0, timer = 0, styleFrame = 0;
    let currentStyle = theme, fitPending = true, styleReady = false, attempt = 0;
    let relief = startupGlobe || pref('relief') === 'on', reliefFailed = false, fieldsVisible = false;
    let fields = {type:'FeatureCollection',features:[]}, airportPin;
    const clearPin = () => { airportPin?.remove(); airportPin=null; };
    const reducedMotion = root.matchMedia('(prefers-reduced-motion: reduce)');
    const announce = () => onState({ ready, failed, mode, rotating, relief:relief && !reliefFailed, fieldsVisible, hasFields:fields.features.length > 0, hasRoute: flight?.showRoute !== false && routeCoordinates(flight?.departure, flight?.arrival).length > 0 });
    const message = (text, state) => { status.textContent = text; status.dataset.state = state; };
    const mapDescription = () => relief && !reliefFailed ? 'NASA relief overview · vector detail at close zoom' : 'Vector basemap · online';

    function drawRelief() {
      if (!map || !styleReady) return;
      if (!map.getSource('pilot-relief')) {
        // Public, historical Blue Marble composite, not live imagery or elevation data.
        // Visible tiles only; no bulk/offline download. Vector detail replaces it above z8.
        map.addSource('pilot-relief', {
          type:'raster', tileSize:256, maxzoom:8,
          tiles:['https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/BlueMarble_ShadedRelief_Bathymetry/default/GoogleMapsCompatible_Level8/{z}/{y}/{x}.jpeg'],
          attribution:'Relief: <a href="https://earthobservatory.nasa.gov/features/BlueMarble" target="_blank" rel="noopener">NASA Blue Marble</a> / <a href="https://www.earthdata.nasa.gov/data/tools/gibs" target="_blank" rel="noopener">GIBS</a>'
        });
        const labels = map.getStyle().layers.find(layer => layer.type === 'symbol');
        map.addLayer({ id:'pilot-relief', type:'raster', source:'pilot-relief',
          layout:{ visibility:relief && !reliefFailed ? 'visible' : 'none' },
          paint:{ 'raster-saturation':-.55, 'raster-brightness-max':currentStyle === 'night' ? .62 : .9,
            'raster-opacity':['interpolate',['linear'],['zoom'],7.5,1,9,0], 'raster-fade-duration':200 }
        }, labels?.id);
      } else map.setLayoutProperty('pilot-relief', 'visibility', relief && !reliefFailed ? 'visible' : 'none');
    }
    function drawEnvironment(){
      if(!map||!styleReady)return;
      map.setSky?.({'sky-color':mode==='globe'?'#010204':currentStyle==='day'?'#dae8f4':'#090d14','horizon-color':'#829ebd','fog-color':'#192b40','atmosphere-blend':mode==='globe'?1:0});
      if(map.setTerrain){
        if(terrain&&!terrainFailed){
          if(!map.getSource('pilot-elevation'))map.addSource('pilot-elevation',{type:'raster-dem',tiles:['https://tiles.mapterhorn.com/{z}/{x}/{y}.webp'],tileSize:512,maxzoom:12,encoding:'terrarium',attribution:'Terrain: <a href="https://mapterhorn.com/attribution/" target="_blank" rel="noopener">Mapterhorn / open-data contributors</a>'});
          map.setTerrain({source:'pilot-elevation',exaggeration:terrain});
        }else map.setTerrain(null);
      }
    }
    function setTerrain(value){terrain=[0,1,1.5].includes(Number(value))?Number(value):0;terrainFailed=false;save('terrain',String(terrain));drawEnvironment();}
    function setFavorites(value){favorites=[...new Set(value)].filter(v=>/^[A-Z]{4}$/.test(v)).slice(0,50);save('favorites',JSON.stringify(favorites));drawFavorites();}
    function drawFavorites(){
      if(!map||!styleReady)return;
      const data={type:'FeatureCollection',features:fields.features.filter(f=>favorites.includes(f.properties.icao))};
      if(map.getSource('pilot-favorites'))map.getSource('pilot-favorites').setData(data);
      else{
        map.addSource('pilot-favorites',{type:'geojson',data});
        map.addLayer({id:'pilot-favorite-dot',type:'circle',source:'pilot-favorites',paint:{'circle-radius':5,'circle-color':'#fff','circle-stroke-color':'#132a48','circle-stroke-width':2}});
        map.addLayer({id:'pilot-favorite-label',type:'symbol',source:'pilot-favorites',layout:{'text-field':['get','icao'],'text-font':['Noto Sans Regular'],'text-size':13,'text-offset':[0,1.25]},paint:{'text-color':'#fff','text-halo-color':'#0e203a','text-halo-width':2}});
      }
    }
    function drawTrack(){
      if(!map||!styleReady)return;
      if(map.getSource('pilot-recorded-track'))map.getSource('pilot-recorded-track').setData(recorded);
      else{map.addSource('pilot-recorded-track',{type:'geojson',data:recorded});map.addLayer({id:'pilot-recorded-track',type:'line',source:'pilot-recorded-track',paint:{'line-color':'#5edec3','line-width':3,'line-opacity':.9}});}
    }
    function setTrack(data){recorded=data||{type:'FeatureCollection',features:[]};drawTrack();setRotating(false);}
    function setReplayPosition(point,camera='free'){
      if(!ready||!point)return;
      if(!replayMarker){const n=document.createElement('div');n.className='track-aircraft';n.innerHTML='<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 2 10 10 3 15v2l7-2v5l-2 1v1l4-1 4 1v-1l-2-1v-5l7 2v-2l-7-5z"/></svg>';replayMarker=new root.maplibregl.Marker({element:n,rotationAlignment:'map'}).setLngLat([point.lon,point.lat]).addTo(map);}
      replayMarker.setLngLat([point.lon,point.lat]);replayMarker.setRotation(point.course||0);replayMarker.getElement().classList.toggle('unoriented',point.course==null);
      if(camera!=='free')map.jumpTo({center:[point.lon,point.lat],zoom:Math.max(map.getZoom(),9),pitch:camera==='perspective'?55:0});
    }
    function clearReplay(){replayMarker?.remove();replayMarker=null;setTrack(null);}
    function fitTrack(){
      if(!ready)return;
      const points=recorded.features.flatMap(f=>f.geometry.coordinates);if(!points.length)return;
      map.setPadding(0);const bounds=points.reduce((b,p)=>b.extend(p),new root.maplibregl.LngLatBounds(points[0],points[0]));
      map.fitBounds(bounds,{padding:getPadding(),maxZoom:10,duration:reducedMotion.matches?0:400});
    }
    function saveStartup(){if(!ready)return false;save('startup',JSON.stringify({center:[map.getCenter().lng,map.getCenter().lat],zoom:map.getZoom(),pitch:map.getPitch(),bearing:map.getBearing()}));return true;}
    function setRelief(value) {
      relief = Boolean(value); reliefFailed = false;
      save('relief',relief ? 'on' : 'off');
      drawRelief();
      if (ready) message(mapDescription(), 'ready');
      announce();
    }

    // Optional planning overlay: only catalogued ICAOs with finite coordinates.
    function setAirports(list) {
      const seen = new Set();
      fields = {type:'FeatureCollection',features:[]};
      for (const a of list || []) {
        if (!a || !/^[A-Z]{4}$/.test(a.icao) || seen.has(a.icao) || a.lat == null || a.lon == null || String(a.lat).trim()==='' || String(a.lon).trim()==='') continue;
        const point = coordinates({lat:Number(a.lat),lon:Number(a.lon)});
        if (!point) continue;
        seen.add(a.icao);
        fields.features.push({type:'Feature',properties:{icao:a.icao,name:String(a.name || '').slice(0,80)},geometry:{type:'Point',coordinates:point}});
      }
      map?.getSource('pilot-fields')?.setData(fields);
      drawFields(); drawFavorites(); announce();
    }
    function drawFields() {
      if (!map || !styleReady) return;
      const visibility = fieldsVisible ? 'visible' : 'none';
      if (!map.getSource('pilot-fields')) {
        if (!fieldsVisible || !fields.features.length) return;
        const before = map.getLayer('pilot-route-casing') ? 'pilot-route-casing' : undefined;
        map.addSource('pilot-fields',{type:'geojson',data:fields});
        map.addLayer({id:'pilot-field-dots',type:'circle',source:'pilot-fields',minzoom:5,paint:{'circle-radius':4,'circle-color':'#e7edf8','circle-stroke-color':'#374665','circle-stroke-width':2}},before);
        map.addLayer({id:'pilot-field-labels',type:'symbol',source:'pilot-fields',minzoom:6,layout:{'text-field':['get','icao'],'text-font':['Noto Sans Regular'],'text-size':13,'text-anchor':'left','text-offset':[.7,0]},paint:{'text-color':currentStyle==='night'?'#f1f4f8':'#182232','text-halo-color':currentStyle==='night'?'#090d14':'#f5f7fc','text-halo-width':1.8}},before);
      } else for (const layer of ['pilot-field-dots','pilot-field-labels']) map.setLayoutProperty(layer,'visibility',visibility);
      filterRouteFields();
    }
    function filterRouteFields() {
      if (!styleReady || !map?.getSource('pilot-fields')) return;
      const featured = [coordinates(flight?.departure) && flight?.dep, coordinates(flight?.arrival) && flight?.arr].filter(Boolean);
      const filter = featured.length ? ['!', ['in',['get','icao'],['literal',featured]]] : null;
      for (const id of ['pilot-field-dots','pilot-field-labels']) map.setFilter(id,filter);
    }
    function setFields(value) { fieldsVisible = Boolean(value) && fields.features.length > 0; drawFields(); announce(); }
    function viewAirport(code) {
      const point = fields.features.find(f => f.properties.icao === String(code).trim().toUpperCase());
      if (!ready || !point) return false;
      setMode('map'); setRotating(false); map.stop();
      map.easeTo({center:point.geometry.coordinates,zoom:9,bearing:0,pitch:0,duration:reducedMotion.matches ? 0 : 450});
      clearPin();
      airportPin = new root.maplibregl.Popup({className:'airport-reference',closeOnClick:true,focusAfterOpen:false,maxWidth:'240px'})
        .setLngLat(point.geometry.coordinates).setText([point.properties.icao,point.properties.name].filter(Boolean).join(' · ')).addTo(map);
      return true;
    }

    function stopAnimation() {
      root.cancelAnimationFrame(frame); frame = 0; previousTime = 0;
    }
    function animate(time) {
      frame = 0;
      if (!ready || !active || document.hidden || !rotating || mode !== 'globe' || reducedMotion.matches) return;
      if (previousTime && !map.isMoving()) {
        const center = map.getCenter();
        map.jumpTo({ center: [center.lng + Math.min(time - previousTime, 50) * .0005, center.lat] });
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
      clearPin();
      if (!ready || !active) { fitPending = true; return; }
      fitPending = false;
      setRotating(false);
      const path = routeCoordinates(flight?.departure, flight?.arrival);
      const duration = reducedMotion.matches ? 0 : 650;
      // fitBounds adds its padding to the retained camera padding. Reset it
      // first, otherwise a previously padded globe can leave no fitting area.
      map.setPadding(0);
      if (path.length) {
        const bounds = path.reduce((box, point) => box.extend(point), new root.maplibregl.LngLatBounds(path[0], path[0]));
        try {
          map.fitBounds(bounds, { padding: getPadding(), maxZoom: 8, duration, bearing: 0, pitch: 0 });
        } catch {
          // Rendering failure must never interrupt saving or loading a flight.
          message('Route loaded · adjust map view manually', 'limited');
        }
      } else map.easeTo({ ...HOME, zoom:mode === 'globe' ? .6 : HOME.zoom, bearing:0, pitch:0, ...(startupCamera||{}), duration });
    }
    function drawFlight() {
      if (!map || !styleReady) return;
      const data = flightGeometry(flight);
      const source = map.getSource('pilot-route');
      if (source) source.setData(data);
      else {
        map.addSource('pilot-route', { type: 'geojson', data });
        map.addLayer({ id: 'pilot-route-casing', type: 'line', source: 'pilot-route', filter: ['==', '$type', 'LineString'], paint: { 'line-color': currentStyle === 'night' ? '#0e203a' : '#ffffff', 'line-width': 7, 'line-opacity': .75 } });
        map.addLayer({ id: 'pilot-route-line', type: 'line', source: 'pilot-route', filter: ['==', '$type', 'LineString'], paint: { 'line-color': currentStyle === 'night' ? '#eef2f7' : '#101f3b', 'line-width': 3 } });
        map.addLayer({ id: 'pilot-route-airports', type: 'circle', source: 'pilot-route', filter: ['==', '$type', 'Point'], paint: { 'circle-radius': 7, 'circle-color': ['match', ['get', 'kind'], 'arrival', '#b4bbc8', '#ffffff'], 'circle-stroke-width': 3, 'circle-stroke-color': '#16384a' } });
        map.addLayer({ id: 'pilot-route-labels', type: 'symbol', source: 'pilot-route', filter: ['==', '$type', 'Point'], layout: { 'text-field': ['get', 'label'], 'text-font': ['Noto Sans Regular'], 'text-size': 15, 'text-offset': [0, -1.5] }, paint: { 'text-color': '#ffffff', 'text-halo-color': '#132c3e', 'text-halo-width': 2 } });
      }
    }
    function fail() {
      if (destroyed) return;
      clearPin();
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
        map = new engine.Map({ container, style: STYLES[theme], ...HOME, zoom:mode === 'map' ? HOME.zoom : .6, maxZoom: 14, attributionControl: false, pixelRatio: Math.min(root.devicePixelRatio || 1, 2), renderWorldCopies: false });
        map.addControl(new engine.NavigationControl({ visualizePitch: true }), 'top-right');
        map.addControl(new engine.ScaleControl({unit:'nautical',maxWidth:90}), 'bottom-left');
        map.addControl(new engine.AttributionControl({ compact: true }), 'bottom-right');
        map.on('style.load', () => {
          styleReady = true;
          if (ready) root.clearTimeout(timer);
          if (currentStyle === 'night') {
            // Brighter coastline/labels than the stock dark basemap, without a CSS filter.
            for (const layer of map.getStyle().layers) {
              // The dark style references an absent circle-11 sprite on three city
              // labels. Keep their real text, without a redundant settlement dot.
              if (['place_town','place_city','place_city_large'].includes(layer.id) && JSON.stringify(layer.layout?.['icon-image'] || '').includes('"circle-11"')) map.setLayoutProperty(layer.id,'icon-image',null);
              if (layer.id === 'background') map.setPaintProperty(layer.id, 'background-color', '#090d14');
              if (layer.id === 'water') map.setPaintProperty(layer.id, 'fill-color', '#0d1928');
              if (layer.id === 'landcover_wood') map.setPaintProperty(layer.id,'fill-color','#141c1b');
              if (layer.id === 'landuse_residential') map.setPaintProperty(layer.id,'fill-color','#19202b');
              if (['highway_motorway_inner','highway_motorway_subtle','highway_major_inner','highway_major_subtle'].includes(layer.id)) map.setPaintProperty(layer.id,'line-color','#526072');
              if (layer.id === 'aeroway-runway') map.setPaintProperty(layer.id,'line-color','#d4d4dc');
              if (layer.id === 'landcover_glacier' || layer.id === 'landcover_ice_shelf') map.setPaintProperty(layer.id, 'fill-color', '#496373');
              if (layer.id.startsWith('boundary_country')) map.setPaintProperty(layer.id, 'line-color', '#8494a4');
              if (layer.type === 'symbol' && layer.id.startsWith('place_')) {
                map.setPaintProperty(layer.id, 'text-color', '#f0f0f5');
                map.setPaintProperty(layer.id, 'text-halo-color', '#080d14');
              }
            }
          } else {
            // Use the style's real geometry; keep hazard and operational layers independent.
            for (const layer of map.getStyle().layers) {
              if (layer.id === 'background') map.setPaintProperty(layer.id,'background-color','#f3f2ef');
              if (layer.type === 'fill' && Object.hasOwn(DAY_LAND,layer.id)) map.setPaintProperty(layer.id,'fill-color',DAY_LAND[layer.id]);
              if (layer.type === 'line' && /^(road|bridge|tunnel)_motorway(_link)?$/.test(layer.id)) map.setPaintProperty(layer.id,'line-color','#d7c6a8');
            }
          }
          map.setProjection({ type: mode === 'globe' ? 'globe' : 'mercator' });
          // style.load permits adding sources even while source tiles are loading.
          drawRelief(); drawFields(); drawFlight(); drawFavorites(); drawTrack(); drawEnvironment();
          if (ready) message(mapDescription(), 'ready');
        });
        map.on('load', () => {
          root.clearTimeout(timer); ready = true; starting = false;
          fallback.hidden = true;
          message(mapDescription(), 'ready');
          map.resize();
          if (fitPending) fitRoute();
          if(startupGlobe&&!flight?.dep)setRotating(true);
          setTheme(theme); syncAnimation(); announce();
        });
        map.on('error', event => {
          if(event.sourceId==='pilot-elevation'){terrainFailed=true;map.setTerrain(null);message('Elevation unavailable · basemap retained (not terrain)', 'limited');}
          else if (event.sourceId === 'pilot-relief') {
            reliefFailed = true; drawRelief();
            announce();
            if (ready) message('Relief unavailable · vector map retained', 'limited');
          } else if (ready) message('Some map detail is unavailable · check connection', 'limited');
        });
        map.on('idle', () => { if (ready && navigator.onLine && status.dataset.state !== 'limited') message(mapDescription(), 'ready'); });
        map.on('click',event=>{if(!event.point)return;const layers=['pilot-favorite-dot','pilot-field-dots'].filter(id=>map.getLayer(id));if(!layers.length)return;const feature=map.queryRenderedFeatures(event.point,{layers})[0];if(feature){setRotating(false);onAirport(feature.properties.icao);}});
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
      const nextKey = JSON.stringify([value?.dep, value?.arr, coordinates(value?.departure), coordinates(value?.arrival),value?.showRoute]);
      flight = value;
      if (nextKey === flightKey) return;
      flightKey = nextKey; fitPending = true; chartCamera = null;
      drawFlight(); filterRouteFields(); if (ready) fitRoute(); announce();
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
      const next = value === 'map' ? 'map' : 'globe';
      if (!ready || next === mode) return;
      map.stop();
      if (mode === 'map') chartCamera = {center:map.getCenter(),zoom:map.getZoom(),bearing:map.getBearing(),pitch:map.getPitch()};
      clearPin(); mode = next; save('projection',mode);
      setRotating(false);
      map.setProjection({ type: mode === 'globe' ? 'globe' : 'mercator' });
      drawEnvironment();
      // Keep the flight and camera. Zooming out reveals the spherical projection.
      if (mode === 'globe') map.easeTo({ zoom: Math.min(map.getZoom(), .6), pitch: 0, duration: reducedMotion.matches ? 0 : 450 });
      else if (chartCamera) map.easeTo({...chartCamera,duration:reducedMotion.matches ? 0 : 450});
      else fitRoute();
      announce();
    }
    const resize = new ResizeObserver(() => { if (active && map) map.resize(); });
    resize.observe(container);
    const motionChanged = () => { if (reducedMotion.matches) setRotating(false); };
    reducedMotion.addEventListener('change', motionChanged);
    const connectionChanged = () => {
      if (!navigator.onLine) message('Offline · basemap detail may be incomplete', 'limited');
      else if (failed) { failed = false; if (active) start(); }
      else if (ready && !reliefFailed) {
        message('Connection restored · refreshing map', 'loading');
        map.triggerRepaint();
      }
    };
    root.addEventListener('online', connectionChanged); root.addEventListener('offline', connectionChanged);
    return {
      get ready() { return ready; }, getFavorites:()=>[...favorites], setFavorites,setTerrain,saveStartup,setTrack,fitTrack,setReplayPosition,clearReplay,
      setActive, setFlight, setTheme, setMode, setRotating, setRelief, fitRoute, setAirports, setFields, viewAirport,
      retry() { if (active && failed && !destroyed) { failed=false; announce(); start(); } },
      destroy() { destroyed = true; stopAnimation(); replayMarker?.remove(); root.clearTimeout(timer); root.cancelAnimationFrame(styleFrame); resize.disconnect(); reducedMotion.removeEventListener('change', motionChanged); root.removeEventListener('online', connectionChanged); root.removeEventListener('offline', connectionChanged); map?.remove(); }
    };
  }
  root.PilotMapGeometry = Object.freeze({ coordinates, routeCoordinates, flightGeometry });
  root.createPilotMapView = createPilotMapView;
})(window);

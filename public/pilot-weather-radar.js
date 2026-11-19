/* FMI observed radar. Only visible WMS tiles are requested; no bulk prefetch. */
window.createPilotWeatherRadar = function ({getAirports, isVisible}) {
  const el = id => document.getElementById(id), layers = new Map();
  let map, route, manifest, pending, loadedAt = 0, index = -1, shown = -1, timer, frameTimer, busy = false, generation = 0;
  const opacity = () => Number(el('weatherRadarOpacity').value) / 100;
  const status = text => {el('weatherRadarTime').textContent = text;};
  function stamp() {
    if (!manifest || shown < 0) return;
    const frame = manifest.frames[shown], latest = manifest.frames.at(-1);
    if(layers.get(frame.iso)?.errors){status('RADAR TILES INCOMPLETE · retry');return;}
    const age = Math.max(0, Math.floor((Date.now()/1000 - latest.time)/60));
    status(`${new Date(frame.time*1000).toISOString().slice(11,16)} UTC · ${shown+1}/${manifest.frames.length} · latest ${age} min old${age>20 ? ' · STALE' : ''}`);
  }
  function ensure() {
    if (map) {map.invalidateSize(false); return true;}
    if (!window.L) {status('Map engine unavailable'); return false;}
    map = L.map(el('weatherRadarMap'), {minZoom:2,maxZoom:10,scrollWheelZoom:false,attributionControl:true}).setView([62,26],5);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:19,attribution:'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'}).addTo(map);
    L.control.scale({imperial:false}).addTo(map);
    map.on('movestart', () => play(false));
    return true;
  }
  function updateRoute(fit = false) {
    if (!map) return;
    route?.remove(); route = L.layerGroup().addTo(map);
    const airports = getAirports().filter(Boolean);
    const points = airports.map(a => [a.lat,a.lon]);
    if (el('weatherRadarRouteToggle').checked) {
      if (points.length > 1) L.polyline(points, {color:'#ffe09b',weight:3,dashArray:'7 6'}).addTo(route);
      airports.forEach(a => {const label=document.createElement('span');label.textContent=a.icao;
        L.circleMarker([a.lat,a.lon], {radius:6,color:'#fff',weight:2,fillColor:'#176fbc',fillOpacity:1})
        .bindTooltip(label, {permanent:true,direction:'top'}).addTo(route);});
    }
    if (fit && points.length) map.fitBounds(L.latLngBounds(points).pad(.5), {maxZoom:7,animate:false});
  }
  function play(value) {
    clearInterval(timer); timer = null;
    const playing = Boolean(value && manifest?.frames.length>1 && isVisible() && !document.hidden);
    el('weatherRadarPlayBtn').textContent = playing ? 'Pause' : 'Play recent';
    el('weatherRadarPlayBtn').setAttribute('aria-pressed', String(playing));
    if (playing) {
      if(index>=manifest.frames.length-1)frame(0);
      // One pass ends on the latest observation. Never consume the free
      // provider's tile allowance with an unattended, endless replay loop.
      timer = setInterval(() => {
        if(busy)return;
        if(index>=manifest.frames.length-1)play(false);else frame(index+1);
      },1200);
    }
  }
  function frame(value) {
    if (!manifest || !ensure()) return;
    const next = Math.max(0,Math.min(manifest.frames.length-1,Number(value)||0));
    const token = ++generation; index = next; busy = true; clearTimeout(frameTimer);
    const observation = manifest.frames[index];
    el('weatherRadarTimeline').value = String(index);
    status(`Loading ${observation.iso.slice(11,16)} UTC…`);
    let entry = layers.get(observation.iso);
    function commit() {
      if (token !== generation) return;
      busy = false; clearTimeout(frameTimer);
      if (entry.errors) {
        entry.layer.remove(); layers.delete(observation.iso); play(false);
        el('weatherRadarUnavailable').hidden = false;
        el('weatherRadarUnavailable').textContent = 'Radar tiles incomplete. Previous frame retained if available; retry or use an approved source.';
        if (shown >= 0) stamp(); else status('RADAR UNAVAILABLE');
        return;
      }
      for (const item of layers.values()) item.layer.setOpacity(item === entry ? opacity() : 0);
      entry.layer.bringToFront(); shown = next; stamp(); el('weatherRadarUnavailable').hidden = true;
      while (layers.size > 3) {
        const key = [...layers.keys()].find(key => key !== observation.iso);
        layers.get(key).layer.remove(); layers.delete(key);
      }
    }
    if (entry?.loaded) {commit(); return;}
    if (!entry) {
      const layer = L.tileLayer.wms(manifest.host, {layers:manifest.layer,styles:manifest.style,format:'image/png',transparent:true,
        version:'1.1.1',time:observation.iso,elevation:600,tileSize:256,opacity:0,zIndex:350,bounds:manifest.bounds,
        maxNativeZoom:8,maxZoom:10,updateWhenIdle:true,keepBuffer:1,
        attribution:'Radar: <a href="https://www.ilmatieteenlaitos.fi/avoin-data-lisenssi">FMI · CC BY 4.0</a>'});
      entry = {layer,loaded:false,errors:0}; layers.set(observation.iso,entry);
      layer.on('loading', () => {entry.loaded=false;entry.errors=0;});
      layer.on('tileerror', () => {entry.errors++;});
      layer.on('load', () => {entry.loaded=true; if (shown === next && entry.errors) {status('RADAR TILES INCOMPLETE'); play(false);}});
    }
    entry.layer.once('load', commit); if (!map.hasLayer(entry.layer)) entry.layer.addTo(map);
    frameTimer = setTimeout(() => {if (token === generation) {entry.errors++;commit();}},12000);
  }
  async function load(force = false) {
    if (!ensure()) return;
    updateRoute(false);
    if (pending) return pending;
    if (!force && manifest && Date.now()-loadedAt<120000) {stamp();return;}
    play(false); el('weatherRadarRefreshBtn').disabled = true; status('Loading FMI observations…');
    pending = (async () => {
      try {
        const response = await fetch('/api/booking-ops/weather-radar', {cache:'no-store',signal:AbortSignal.timeout(10000)});
        const data = await response.json();
        if (!response.ok || data.host !== 'https://openwms.fmi.fi/geoserver/wms' || !data.frames?.length) throw new Error(data.error || 'Radar metadata unavailable');
        manifest = data; loadedAt = Date.now();
        el('weatherRadarLegend').src = data.legend;
        el('weatherRadarTimeline').max = String(data.frames.length-1);
        frame(data.frames.length-1);
      } catch (error) {
        status(manifest ? 'Radar refresh failed · previous observations' : 'RADAR UNAVAILABLE');
        el('weatherRadarUnavailable').textContent = `${error.message}. METAR / TAF remains available.`;
        el('weatherRadarUnavailable').hidden = false;
      } finally {el('weatherRadarRefreshBtn').disabled=false; pending=null;}
    })();
    return pending;
  }
  el('weatherRadarLatest').addEventListener('click', () => {play(false);frame(manifest?.frames.length-1);});
  el('weatherRadarOpacity').addEventListener('input', () => {const entry=layers.get(manifest?.frames[shown]?.iso);entry?.layer.setOpacity(opacity());});
  el('weatherRadarRouteToggle').addEventListener('change', () => updateRoute());
  document.addEventListener('visibilitychange', () => {if(document.hidden)play(false);});
  return {load,play,frame,updateRoute,refreshAge:()=>{if(isVisible()&&!busy)stamp();},resize:()=>map?.invalidateSize(false),step:delta=>{play(false);frame(index+delta);}};
};

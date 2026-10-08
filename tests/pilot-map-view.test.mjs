import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../public/pilot-map-view.js', import.meta.url), 'utf8');
function harness() {
  const frames = new Map(), timers = new Map(), listeners = new Map(), maps = [];
  let id = 0;
  const motion = { matches:false, addEventListener(){}, removeEventListener(){} };
  class MapStub {
    constructor(options) { this.options=options; this.events=new Map(); this.sources=new Map(); this.fits=0; this.styles=[]; maps.push(this); }
    on(name, fn) { this.events.set(name, [...(this.events.get(name)||[]), fn]); }
    emit(name, event={}) { this.events.get(name)?.forEach(fn=>fn(event)); }
    addControl() {} resize() {} setProjection(value) { this.projection=value; }
    isStyleLoaded() { return true; }
    getStyle() { return {layers:[{id:'background',type:'background'},{id:'water',type:'fill'},{id:'place_city',type:'symbol'}]}; }
    setPaintProperty() {}
    getSource(name) { return this.sources.get(name); }
    addSource(name, value) { this.sources.set(name, { data:value.data, setData(data){this.data=data;} }); }
    addLayer() {}
    setStyle(value) { this.styles.push(value); this.sources.clear(); this.emit('style.load'); }
    fitBounds(bounds, options) { this.fits++; this.fit={bounds,options}; }
    easeTo(value) { this.camera=value; }
    getZoom() { return 7; }
    isMoving() { return false; }
    getCenter() { return {lng:24,lat:60}; }
    jumpTo(value) { this.camera=value; }
    remove() { this.removed=true; }
  }
  class Bounds { constructor() {this.points=[];} extend(p) {this.points.push(p);return this;} }
  const window = {
    maplibregl:{ Map:MapStub, NavigationControl:class {}, AttributionControl:class {}, LngLatBounds:Bounds, setWorkerUrl(){} },
    matchMedia:()=>motion, devicePixelRatio:3,
    requestAnimationFrame:fn=>{frames.set(++id,fn);return id;}, cancelAnimationFrame:key=>frames.delete(key),
    setTimeout:fn=>{timers.set(++id,fn);return id;},clearTimeout:key=>timers.delete(key),
    addEventListener:(name,fn)=>listeners.set(name,fn), removeEventListener:name=>listeners.delete(name)
  };
  const document={hidden:false,documentElement:{dataset:{efbTheme:'night'}}};
  const navigator={onLine:true};
  vm.runInNewContext(source,{window,document,navigator,ResizeObserver:class {observe(){}disconnect(){}}});
  const container={hidden:true},fallback={hidden:false},status={dataset:{}},states=[];
  const adapter=window.createPilotMapView({container,fallback,status,onState:state=>states.push(state)});
  const flush=()=>{const queued=[...frames.values()];frames.clear();queued.forEach(fn=>fn(50));};
  const load=async()=>{adapter.setActive(true);await Promise.resolve();maps[0].emit('style.load');maps[0].emit('load');flush();};
  return {adapter,geometry:window.PilotMapGeometry,maps,frames,timers,document,navigator,motion,listeners,container,fallback,status,states,flush,load};
}
const route={dep:'EFHV',arr:'EFHN',departure:{lon:24.88,lat:60.65},arrival:{lon:22.96,lat:59.85}};

test('coordinate validation rejects missing, NaN, out-of-range and string data',()=>{
  const {geometry}=harness();
  for(const value of [null,{}, {lon:NaN,lat:0},{lon:0,lat:91},{lon:181,lat:0},{lon:'24',lat:60}]) assert.equal(geometry.coordinates(value),null);
  assert.equal(JSON.stringify(geometry.coordinates({lon:0,lat:0})),'[0,0]');
});
test('great-circle route keeps endpoints and uses the short dateline crossing',()=>{
  const {geometry}=harness();
  const points=geometry.routeCoordinates({lon:179,lat:20},{lon:-179,lat:20});
  assert.equal(points.length,65);
  assert.ok(Math.abs(points[0][0]-179)<1e-8);
  assert.ok(Math.abs(points.at(-1)[0]-181)<1e-8);
  for(let i=1;i<points.length;i++) assert.ok(Math.abs(points[i][0]-points[i-1][0])<1);
  assert.equal(geometry.routeCoordinates({lon:0,lat:0},{lon:180,lat:0}).length,0);
});
test('missing airports do not create a made-up line and labels remain plain GeoJSON strings',()=>{
  const {geometry}=harness();
  const data=geometry.flightGeometry({...route,departure:null,arr:'<script>'});
  assert.equal(data.features.length,1);
  assert.equal(data.features[0].geometry.type,'Point');
  assert.equal(data.features[0].properties.label,'<script>');
});
test('map is lazy, retained across workspaces, and duplicate flight refresh preserves camera',async()=>{
  const h=harness(); assert.equal(h.maps.length,0);
  h.adapter.setFlight(route); await h.load();
  assert.equal(h.maps.length,1); assert.equal(h.fallback.hidden,true); assert.equal(h.maps[0].fits,1);
  assert.equal(h.maps[0].options.pixelRatio,2);
  h.adapter.setFlight({...route}); assert.equal(h.maps[0].fits,1);
  h.adapter.setActive(false); h.adapter.setActive(true);h.flush();
  assert.equal(h.maps.length,1);assert.equal(h.maps[0].fits,1);
  h.adapter.setFlight({...route,arr:'EFHK',arrival:{lon:24.96,lat:60.32}});assert.equal(h.maps[0].fits,2);
});
test('theme switches keep the map and reinstate the selected route after style loading',async()=>{
  const h=harness();h.adapter.setFlight(route);await h.load();
  h.maps[0].isStyleLoaded = () => false; // Tiles still loading at style.load.
  h.adapter.setTheme('day');h.flush();
  assert.equal(h.maps.length,1);assert.equal(h.maps[0].styles.length,1);
  assert.equal(h.maps[0].getSource('pilot-route').data.features.length,3);
  assert.equal(h.maps[0].fits,1);
  h.adapter.setMode('map');assert.equal(h.maps[0].projection.type,'mercator');
  h.adapter.setMode('globe');assert.equal(h.maps[0].projection.type,'globe');
});
test('rotation is opt-in, stops offscreen, on touch, and for reduced motion',async()=>{
  const h=harness();await h.load();assert.equal(h.frames.size,0);
  h.adapter.setRotating(true);assert.equal(h.frames.size,1);
  h.adapter.setActive(false);assert.equal(h.frames.size,0);
  h.adapter.setActive(true);h.flush();assert.equal(h.frames.size,1);
  h.maps[0].emit('dragstart',{originalEvent:{}});assert.equal(h.frames.size,0);
  h.motion.matches=true;h.adapter.setRotating(true);assert.equal(h.frames.size,0);
});
test('WebGL failure restores the fallback and reconnect retries without losing route',async()=>{
  const h=harness();h.adapter.setFlight(route);await h.load();
  h.maps[0].emit('webglcontextlost');assert.equal(h.adapter.ready,false);assert.equal(h.fallback.hidden,false);assert.equal(h.container.hidden,true);
  assert.match(h.status.textContent,/unavailable/);
  h.listeners.get('online')();await Promise.resolve();assert.equal(h.maps.length,2);
  h.maps[1].emit('style.load');h.maps[1].emit('load');assert.equal(h.maps[1].fits,1);
  h.adapter.destroy();assert.equal(h.maps[1].removed,true);assert.equal(h.frames.size,0);
});
test('new production resources are versioned and preview remains separate',()=>{
  const html=fs.readFileSync(new URL('../views/booking-ops.html',import.meta.url),'utf8');
  const sw=fs.readFileSync(new URL('../public/pilot-sw.js',import.meta.url),'utf8');
  const version=sw.match(/PILOT_STYLESHEET_VERSION = '([^']+)'/)[1];
  for(const file of ['pilot-premium.css','pilot-map-view.js','pilot-display.js']) {
    assert.ok(html.includes(`/${file}?v=${version}`));assert.ok(sw.includes(`/${file}?v=\${PILOT_STYLESHEET_VERSION}`));
  }
  assert.ok(!html.includes('/efb-preview/'));
  assert.match(html,/Overview only · direct route · not for navigation/);
  assert.match(html,/dragging:true,\s+touchZoom:true/);
});

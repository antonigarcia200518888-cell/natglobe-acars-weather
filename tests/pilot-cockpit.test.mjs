import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const read = path => fs.readFileSync(new URL('../'+path,import.meta.url),'utf8');
const source = read('public/pilot-cockpit.js');
function harness() {
  const nodes = new Map(), events = {}, tasks = new Map(); let next = 0, onFix, onError, cleared = 0;
  const get = id => { if (!nodes.has(id)) nodes.set(id,{textContent:'',dataset:{},hidden:false,open:false,events:{},setAttribute(){},addEventListener(k,fn){this.events[k]=fn;},showModal(){this.open=true;},close(){this.open=false;}});return nodes.get(id); };
  const document = {hidden:false,getElementById:get,addEventListener(k,fn){events[k]=fn;}};
  const window = {isSecureContext:true,addEventListener(k,fn){events[k]=fn;}};
  const navigator = {geolocation:{watchPosition(success,error){onFix=success;onError=error;return 10;},clearWatch(){cleared++;}}};
  const ctx = vm.createContext({window,document,navigator,Date,Math,Number,setTimeout(fn,delay){tasks.set(++next,{fn,delay});return next;},clearTimeout(id){tasks.delete(id);}});
  vm.runInContext(source,ctx);
  window.createPilotCockpit();
  return {window,document,get,events,tasks,fix:p=>onFix(p),error:e=>onError(e),cleared:()=>cleared};
}
test('location precision distinguishes absent, invalid, stale and low-precision fixes',()=>{
  const h=harness(), health=h.window.PilotCockpit.positionHealth;
  assert.equal(health(null,100000).label,'No fix');
  assert.equal(health({accuracy:-1,timestamp:100000},100000).tone,'caution');
  assert.equal(health({accuracy:5,timestamp:80000},100000).label,'Fix stale');
  assert.equal(health({accuracy:5,timestamp:105000},100000).label,'Fix stale');
  assert.equal(health({accuracy:51,timestamp:100000},100000).tone,'caution');
  assert.equal(health({accuracy:12,timestamp:100000},100000).label,'±12 m');
});
test('clock aligns to second boundaries and stops while hidden',()=>{
  const h=harness(); assert.match(h.get('clock').textContent,/^\d{2}:\d{2}:\d{2} Z$/);
  assert.equal(h.window.PilotCockpit.secondDelay(12001),999);
  assert.equal(h.tasks.size,1);h.document.hidden=true;h.events.visibilitychange();assert.equal(h.tasks.size,0);
  h.document.hidden=false;h.events.visibilitychange();assert.equal(h.tasks.size,1);
});
test('location is opt-in, coalesces bursts and ignores callbacks after stopping',()=>{
  const h=harness();assert.equal(h.get('cockpitLocationValue').textContent,'Location off');
  h.get('cockpitLocationStart').events.click();
  for(let i=0;i<1000;i++) h.fix({coords:{latitude:60,longitude:25,accuracy:i%40},timestamp:Date.now()});
  assert.equal([...h.tasks.values()].filter(t=>t.delay===200).length,1);
  h.get('cockpitLocationStop').events.click();assert.equal(h.cleared(),1);
  h.fix({coords:{latitude:60,longitude:25,accuracy:3},timestamp:Date.now()});
  assert.equal(h.get('cockpitLocationValue').textContent,'Location off');
  assert.equal([...h.tasks.values()].filter(t=>t.delay===200).length,0);
});
test('permission denied clears the fix and offers explicit retry',()=>{
  const h=harness();h.get('cockpitLocationStart').events.click();h.error({code:1});
  assert.equal(h.get('cockpitLocationValue').textContent,'Permission denied');assert.equal(h.get('cockpitLocationStart').hidden,false);
});
test('new production modules parse and share the cache version',()=>{
  const sw=read('public/pilot-sw.js'), version=sw.match(/PILOT_STYLESHEET_VERSION = '([^']+)'/)[1];
  for(const file of ['pilot-cockpit.js','pilot-touch-controls.js','pilot-ofp-touch.js']) {
    new vm.Script(read('public/'+file));assert.ok(sw.includes(`'/${file}'`));assert.ok(sw.includes('`/'+file+'?v=${PILOT_STYLESHEET_VERSION}`'));
  }
  for(const file of ['views/booking-ops.html','views/operational-flight-plan.html']) {
    const html=read(file);for(const match of html.matchAll(/\/pilot-[\w-]+\.(?:css|js)\?v=([^"']+)/g))assert.equal(match[1],version);
  }
  assert.doesNotMatch(source,/localStorage|fetch\(/,'device location must not be persisted or uploaded');
});

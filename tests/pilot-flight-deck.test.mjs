import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source = fs.readFileSync(new URL('../public/pilot-flight-deck.js', import.meta.url), 'utf8');
const ops = fs.readFileSync(new URL('../views/booking-ops.html', import.meta.url), 'utf8');
const ofp = fs.readFileSync(new URL('../views/operational-flight-plan.html', import.meta.url), 'utf8');

function harness() {
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, { value:'',textContent:'',dataset:{},events:{},open:false,disabled:false,hidden:false,
      addEventListener(k,v){this.events[k]=v;},showModal(){this.open=true;},close(){if(this.open){this.open=false;this.events.close?.();}},focus(){},setAttribute(){} });
    return nodes.get(id);
  };
  const window={};
  const ctx=vm.createContext({window,document:{getElementById:node,querySelectorAll:()=>[]}});
  vm.runInContext(source,ctx);
  const state={requestId:'TEST-A',dirty:false,busy:false};
  let saves=0, saveError='', remainDirty=false;
  const planner={getState:()=>state,save:async()=>{saves++;if(saveError)throw Error(saveError);state.dirty=remainDirty;}};
  const flights=[{id:'TEST-A',dep:'EFHV',arr:'EFHN',requestDate:'2026-10-06',requestTime:'10:00',aircraft:'PA28',manualFlight:true},{id:'TEST-B',dep:'EFHK',arr:'EFTP',requestDate:'2026-10-07'}];
  const deck=window.createPilotFlightDeck({getFlights:()=>flights,getSelected:()=>flights[0],getPlanner:()=>planner,choose:()=>{},create:()=>{},today:()=> '2026-10-06',escapeHtml:s=>String(s).replaceAll('<','&lt;').replaceAll('"','&quot;')});
  return {deck,node,state,window,flights,get saves(){return saves;},fail(message){saveError=message;},remainDirty(){remainDirty=true;}};
}

test('flight search matches multiple operational terms and scopes without changing selection',()=>{
  const h=harness();
  assert.equal(h.window.pilotFlightMatches(h.flights[0],'efhv pa28','TODAY','2026-10-06'),true);
  assert.equal(h.window.pilotFlightMatches(h.flights[1],'','TODAY','2026-10-06'),false);
  assert.equal(h.window.pilotFlightMatches(h.flights[1],'','MANUAL'),false);
  assert.equal(h.window.pilotFlightMatches(h.flights[0],'unknown'),false);
  h.deck.open();assert.equal(h.node('flightPickerDialog').open,true);
  assert.match(h.node('flightPickerResults').innerHTML,/aria-current="true"/);
  h.node('flightPickerSearch').value='no-such-flight';h.deck.render();
  assert.match(h.node('flightPickerResults').innerHTML,/No matching flights/);
});

test('flight switcher escapes server-provided metadata',()=>{
  const h=harness();h.flights[0].dep='<script>';h.flights[0].id='" onclick="oops';h.deck.render();
  assert.doesNotMatch(h.node('flightPickerResults').innerHTML,/<script>/);
  assert.match(h.node('flightPickerResults').innerHTML,/&quot; onclick=&quot;oops/);
});

test('clean plans continue immediately and cancelling a dirty switch never saves',async()=>{
  const h=harness();assert.equal(await h.deck.ensureSaved(),true);
  h.state.dirty=true;const result=h.deck.ensureSaved();assert.equal(h.node('flightChangeDialog').open,true);
  assert.equal(await h.deck.ensureSaved(),false,'rapid second action cannot queue another transition');
  h.node('flightChangeCancelBtn').events.click();
  assert.equal(await result,false);assert.equal(h.saves,0);assert.equal(h.state.dirty,true);
});

test('Save and continue waits for successful save, and failed saves keep the flight open',async()=>{
  const h=harness();h.state.dirty=true;h.fail('Network unavailable');const result=h.deck.ensureSaved();
  await h.node('flightChangeSaveBtn').events.click();assert.equal(h.node('flightChangeDialog').open,true);
  assert.match(h.node('flightChangeMessage').textContent,/Network unavailable.*current flight remains open/);
  assert.equal(h.node('flightChangeSaveBtn').disabled,false);
  h.fail('');await h.node('flightChangeSaveBtn').events.click();
  assert.equal(await result,true);assert.equal(h.node('flightChangeDialog').open,false);
});

test('new edits during a save prevent flight switching; saving state blocks duplicate actions',async()=>{
  const h=harness();h.state.dirty=true;h.remainDirty();const result=h.deck.ensureSaved();
  await h.node('flightChangeSaveBtn').events.click();assert.equal(h.node('flightChangeDialog').open,true);
  assert.match(h.node('flightChangeMessage').textContent,/New edits/);
  h.node('flightChangeCancelBtn').events.click();assert.equal(await result,false);
  h.state.busy=true;const busy=h.deck.ensureSaved();assert.equal(h.node('flightChangeSaveBtn').disabled,true);
  h.state.busy=false;h.deck.updateDraftStatus();assert.equal(h.node('flightChangeSaveBtn').disabled,false);
  h.node('flightChangeCancelBtn').events.click();assert.equal(await busy,false);
});

test('server save responses do not overwrite edits entered during the request',async()=>{
  const start=ofp.indexOf('    async function save() {');
  const end=ofp.indexOf("    document.querySelectorAll('.movement-button')",start);
  let finishResponse, populated=0;
  const ctx=vm.createContext({saveBusy:false,movementBusy:false,editRevision:1,savedRevision:0,saved:false,
    requestId:'TEST-A',plan:{route:'EFHV DCT EFHN'},request:{},readPlan:()=>{},calculations:()=>({warnings:[]}),
    setSaveState:()=>{},el:()=>({}),fetch:()=>new Promise(resolve=>{finishResponse=resolve;}),
    setReleasePanel:()=>{},populate:async()=>{populated++;},embedded:false,Date,JSON,encodeURIComponent});
  vm.runInContext(ofp.slice(start,end),ctx);const saving=vm.runInContext('save()',ctx);
  ctx.editRevision=2;ctx.plan.route='NEW UNSAVED ROUTE';
  finishResponse({ok:true,json:async()=>({request:{operationalFlightPlan:{route:'OLD SERVER ROUTE'}}})});
  await saving;assert.equal(ctx.plan.route,'NEW UNSAVED ROUTE');assert.equal(ctx.saved,false);
  assert.equal(ctx.savedRevision,1);assert.equal(ctx.saveBusy,false);assert.equal(populated,0);
});

test('flight refresh keeps current selection, then restores the last selected reference on reload',async()=>{
  const start=ops.indexOf('    async function loadRequests() {');
  const end=ops.indexOf('    async function refreshSelectedRequest()',start);
  const flights=[{id:'A'},{id:'B'}];
  const ctx=vm.createContext({selected:flights[1],requests:[],mode:{},activeWorkspace:'flights',ACTIVE_FLIGHT_STORAGE_KEY:'active',
    fetch:async()=>({ok:true,json:async()=>({requests:flights})}),localStorage:{getItem:()=> 'B'},
    renderCrewFilterOptions:()=>{},renderOpsTotals:()=>{},renderList:()=>{},renderCalendar:()=>{},
    sortQueue:list=>list,selectRequest:flight=>{ctx.selected=flight;},readOperationalSnapshot:()=>null,activePlanner:()=>null});
  vm.runInContext(ops.slice(start,end),ctx);
  await vm.runInContext('loadRequests()',ctx);assert.equal(ctx.selected.id,'B');
  ctx.selected=null;await vm.runInContext('loadRequests()',ctx);assert.equal(ctx.selected.id,'B');
  ctx.localStorage.getItem=()=> 'MISSING';ctx.selected=null;
  await vm.runInContext('loadRequests()',ctx);assert.equal(ctx.selected.id,'A');
  ctx.selected={id:'REMOVED'};ctx.activePlanner=()=>({getState:()=>({dirty:true})});
  await vm.runInContext('loadRequests()',ctx);assert.equal(ctx.selected.id,'REMOVED');
  assert.match(ctx.mode.textContent,/DRAFT PRESERVED/);
});

test('flight controls have unique ids and the flight deck is versioned with its service worker',()=>{
  const markup=ops.replace(/<script[\s\S]*?<\/script>/g,'');
  const ids=[...markup.matchAll(/\sid="([^"]+)"/g)].map(m=>m[1]);assert.equal(ids.length,new Set(ids).size);
  const worker=fs.readFileSync(new URL('../public/pilot-sw.js',import.meta.url),'utf8');
  const version=worker.match(/PILOT_STYLESHEET_VERSION = '([^']+)'/)[1];
  assert.ok(ops.includes(`/pilot-flight-deck.js?v=${version}`));
  assert.ok(worker.includes('`/pilot-flight-deck.js?v=${PILOT_STYLESHEET_VERSION}`'));
});

test('a slow background refresh cannot switch back to the previous flight',async()=>{
  const start=ops.indexOf('    async function refreshSelectedRequest() {');
  const end=ops.indexOf('    async function autoSyncOps()',start);
  let respond;
  const ctx=vm.createContext({selected:{id:'A'},requests:[],fetch:()=>new Promise(resolve=>{respond=resolve;}),
    renderCrewFilterOptions:()=>{},renderOpsTotals:()=>{},renderCalendar:()=>{},renderList:()=>{},
    selectRequest:flight=>{ctx.selected=flight;}});
  vm.runInContext(ops.slice(start,end),ctx);const refreshing=vm.runInContext('refreshSelectedRequest()',ctx);
  ctx.selected={id:'B'};respond({ok:true,json:async()=>({requests:[{id:'A'},{id:'B'}]})});
  assert.equal(await refreshing,null);assert.equal(ctx.selected.id,'B');
});

test('PDF actions do not present an earlier saved revision as the current plan',async()=>{
  let opened=false,closed=false;
  const nodes=new Map();const el=id=>{if(!nodes.has(id))nodes.set(id,{});return nodes.get(id);};
  const popup={closed:false,close(){closed=true;},location:{replace(){opened=true;}}};
  const ctx=vm.createContext({saved:false,save:async()=>true,window:{open:()=>popup},el,
    setSaveState:()=>{},setInspector:()=>{},setPdfPreviewBusy:()=>{},previewLoadToken:0,previewLoadTimeout:0});
  const previewStart=ofp.indexOf('    async function refreshPdfPreview() {');
  const openStart=ofp.indexOf('    async function openCurrentPdf() {');
  const end=ofp.indexOf('    function ',openStart+1);
  vm.runInContext(ofp.slice(previewStart,end),ctx);
  await vm.runInContext('refreshPdfPreview()',ctx);
  assert.match(el('pdfPreviewState').textContent,/Newer edits remain unsaved/);
  await vm.runInContext('openCurrentPdf()',ctx);
  assert.equal(opened,false);assert.equal(closed,true);
});

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../views/operational-flight-plan.html', import.meta.url), 'utf8');
const markup = source.replace(/<script[\s\S]*?<\/script>/g, '');
const functionSource = (name, nextName) => {
  const start = source.indexOf(`    function ${name}(`);
  return source.slice(start, source.indexOf(`    ${nextName}`, start + 1));
};

test('all EFB inline scripts parse', () => {
  for (const file of ['operational-flight-plan.html', 'booking-ops.html']) {
    const html = fs.readFileSync(new URL(`../views/${file}`, import.meta.url), 'utf8');
    for (const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
  }
});

test('planner has one stylesheet, eight folder sections, and no duplicate IDs or fields', () => {
  for (const attribute of ['id', 'data-field']) {
    const values = [...markup.matchAll(new RegExp(`\\s${attribute}="([^"]+)"`, 'g'))].map(m => m[1]);
    assert.equal(values.length, new Set(values).size, `duplicate ${attribute}`);
  }
  assert.equal([...markup.matchAll(/role="tab" data-workspace=/g)].length, 8);
  assert.equal([...markup.matchAll(/rel="stylesheet"/g)].length, 1);
  assert.doesNotMatch(source, /foreflight/i);
  for (const field of ['departure','destination','route','depRunway','arrRunway','estimatedEnrouteMinutes','fuelFlowGph','tripFuelGal','finalReserveFuelGal','crew1Lb','passengerWeightOverrideLb','releaseAccepted','actualOut','actualOff','actualOn','actualIn']) {
    assert.ok(markup.includes(`data-field="${field}"`), `missing ${field}`);
  }
  assert.match(source, /BRIEF:\['dispatchSection', 'releaseSection'\]/);
  assert.match(source, /workspaceOrder = \['SUMMARY', 'SETUP', 'ROUTE', 'TIME', 'LOAD', 'BRIEF', 'FLY', 'FILES'\]/);
  for (const section of ['flightSummarySection','timeSummarySection','folderDocumentsSection']) assert.ok(markup.includes(`id="${section}"`));
  assert.match(source,/localStorage.getItem\(ofpWorkspaceStorageKey\) \|\| 'SUMMARY'/);
});

function routeContext() {
  const inputs = Object.fromEntries(Object.entries({ departure:'EFHV', destination:'EFHN', depRunway:'04', arrRunway:'03', departureProcedure:'OLD SID', arrivalProcedure:'OLD STAR', approachProcedure:'OLD APP' }).map(([key,value]) => [key,{value}]));
  inputs.releaseAccepted = {checked:true};
  const nodes = {routeTokenInput:{value:'NIKUN'}};
  const ctx = vm.createContext({
    routeLegs:[{ident:'EFHV'},{ident:'DCT'},{ident:'EFHN'}],
    inputFor:name => inputs[name], el:name => nodes[name],
    inferRouteLeg:(ident,type) => ({ident,type}),
    setRouteDataSource:()=>{}, rememberRouteLeg:()=>{}, renderRouteHistory:()=>{},
    syncRouteFields:()=>{}, markUnsaved:()=>{}, setSaveState:()=>{}, renderRouteComposer:()=>{}, render:()=>{},
    readPlan:()=>{}, populateRunwaySelectors:async()=>{},
  });
  return {ctx,inputs};
}

test('waypoints are inserted before arrival and direct legs are not duplicated', () => {
  const {ctx,inputs} = routeContext();
  vm.runInContext(functionSource('addRouteElement', 'async function searchNavigationReference'), ctx);
  vm.runInContext("addRouteElement('NIKUN')", ctx);
  assert.equal(ctx.routeLegs.map(l=>l.ident).join(' '), 'EFHV DCT NIKUN EFHN');
  assert.equal(inputs.releaseAccepted.checked, false);
  vm.runInContext("addRouteElement('DCT', {type:'DCT'}); addRouteElement('DCT', {type:'DCT'});", ctx);
  assert.equal(ctx.routeLegs.map(l=>l.ident).join(' '), 'EFHV DCT NIKUN DCT EFHN');
});

test('swap reverses the route with its endpoints and clears directional procedures', async () => {
  const {ctx,inputs} = routeContext();
  const start = source.indexOf("el('swapAirportsBtn').addEventListener('click', async () => {");
  const end = source.indexOf("    el('routeTokenAddBtn')", start);
  const handler = source.slice(start,end).replace(/^el\('swapAirportsBtn'\)\.addEventListener\('click', /,'(').replace(/\);\s*$/,' )()');
  await vm.runInContext(handler,ctx);
  assert.equal(inputs.departure.value,'EFHN');
  assert.equal(inputs.destination.value,'EFHV');
  assert.equal(ctx.routeLegs.map(l=>l.ident).join(' '),'EFHN DCT EFHV');
  assert.equal(inputs.depRunway.value,'03');
  assert.equal(inputs.departureProcedure.value,'');
  assert.equal(inputs.arrivalProcedure.value,'');
  assert.equal(inputs.approachProcedure.value,'');
  assert.equal(inputs.releaseAccepted.checked,false);
});

test('unavailable runway catalogue does not prevent manual runway entry', () => {
  const nodes = {options:{innerHTML:''},help:{textContent:''}};
  const input = {value:'',getAttribute:key => key === 'list' ? 'options' : 'help'};
  const ctx=vm.createContext({el:id=>nodes[id],escapeHtml:value=>String(value),runwayLabel:r=>r.ident});
  vm.runInContext(functionSource('setRunwayOptions','async function populateRunwaySelectors'),ctx);
  ctx.input=input;
  vm.runInContext("setRunwayOptions(input, [], '04')",ctx);
  assert.equal(input.value,'04');
  assert.match(nodes.help.textContent,/Catalogue unavailable/);
  assert.match(nodes.options.innerHTML,/value="04"/);
});

test('service worker ships the new planner stylesheet, not the removed handoff', () => {
  const sw=fs.readFileSync(new URL('../public/pilot-sw.js',import.meta.url),'utf8');
  assert.match(sw,/pilot-ofp-workflow\.css/);
  assert.doesNotMatch(sw,/foreflight/i);
  const version=sw.match(/PILOT_STYLESHEET_VERSION = '([^']+)'/)[1];
  assert.ok(source.includes(`pilot-ofp-workflow.css?v=${version}`));
  const ops=fs.readFileSync(new URL('../views/booking-ops.html',import.meta.url),'utf8');
  for (const match of ops.matchAll(/href="\/(pilot-[^"?]+\.css)\?v=([^"]+)"/g)) assert.equal(match[2],version,match[1]);
});

test('flight-folder tab semantics follow the responsive navigation orientation', () => {
  const attributes={};
  const media={matches:true};
  const ctx=vm.createContext({verticalTaskRailMedia:media,document:{querySelector:()=>({setAttribute:(name,value)=>{attributes[name]=value;}})}});
  vm.runInContext(functionSource('syncTaskRailOrientation','function setInspector'),ctx);
  vm.runInContext('syncTaskRailOrientation()',ctx);
  assert.equal(attributes['aria-orientation'],'vertical');
  media.matches=false;
  vm.runInContext('syncTaskRailOrientation()',ctx);
  assert.equal(attributes['aria-orientation'],'horizontal');
  assert.match(source,/verticalTaskRailMedia\.addEventListener\('change', syncTaskRailOrientation\)/);
});

test('stage changes reset the data sheet, and rotation does not force queue filters open', () => {
  assert.match(functionSource('setWorkspace','function moveWorkspace'),/ofp-app-workspace'\)\?\.scrollTo\(\{ top:0, behavior:'auto' \}\)/);
  const ops=fs.readFileSync(new URL('../views/booking-ops.html',import.meta.url),'utf8');
  assert.match(ops,/<details class="queue-filter-drawer">/);
  assert.doesNotMatch(ops,/queueFilterDrawer\.open\s*=/);
});

test('time summary distinguishes missing, midnight, and invalid movement clocks', () => {
  const ctx=vm.createContext({});
  vm.runInContext(functionSource('movementClocks','function datedMinutesBetween'),ctx);
  assert.equal(vm.runInContext("movementClocks('0000Z/0300L').utc",ctx),'00:00Z');
  assert.equal(vm.runInContext("movementClocks('2359Z/0259L').local",ctx),'02:59L');
  for(const value of ['', '2400Z/0300L', '0060Z/0300L', '0000Z/2500L', '<script>']) {
    ctx.value=value;
    assert.equal(vm.runInContext('movementClocks(value).recorded',ctx),false);
  }
});

test('dated time differences handle midnight and do not invent dates or negative durations', () => {
  const ctx=vm.createContext({});
  vm.runInContext(functionSource('datedMinutesBetween','function renderFlightFolder'),ctx);
  assert.equal(vm.runInContext("datedMinutesBetween('2026-10-02T23:50:00Z','2026-10-03T00:10:00Z')",ctx),20);
  assert.equal(vm.runInContext("datedMinutesBetween('2026-10-02T10:00:00Z','2026-10-02T09:55:00Z',true)",ctx),-5);
  assert.equal(vm.runInContext("datedMinutesBetween('2026-10-02T10:00:00Z','2026-10-02T09:55:00Z')",ctx),null);
  assert.equal(vm.runInContext("datedMinutesBetween('2350Z/0250L','0010Z/0310L')",ctx),null);
  assert.equal(vm.runInContext("datedMinutesBetween('', 'invalid')",ctx),null);
});

test('movement recording preserves unsaved edits and releases the lock after cancellation', async () => {
  const states=[];
  let confirmations=0;
  const ctx=vm.createContext({
    saved:false, movementBusy:false,
    setSaveState:(state,message)=>states.push({state,message}),
    inputFor:()=>({value:''}),
    confirmMovement:async()=>{ confirmations++; return false; },
    fetch:()=>{ throw new Error('Cancelled or unsaved movement must not reach the server'); },
  });
  const start=source.indexOf('    async function recordMovement(');
  const end=source.indexOf("    document.querySelectorAll('[data-stamp-field]')",start);
  vm.runInContext(source.slice(start,end),ctx);
  await vm.runInContext("recordMovement('actualOut','10:05')",ctx);
  assert.equal(confirmations,0);
  assert.match(states[0].message,/planning changes are preserved/);
  ctx.saved=true;
  await vm.runInContext("recordMovement('actualOut','10:05')",ctx);
  assert.equal(confirmations,1);
  assert.equal(ctx.movementBusy,false);
  ctx.movementBusy=true;
  await vm.runInContext("recordMovement('actualOut','10:05')",ctx);
  assert.equal(confirmations,1,'duplicate taps must not create another confirmation');
});

test('movement confirmation uses a named app dialog with explicit cancel and record outcomes', async () => {
  let close;
  let shown=0;
  const dialog={returnValue:'',addEventListener:(event,handler,options)=>{assert.equal(event,'close');assert.equal(options.once,true);close=handler;},showModal:()=>shown++};
  const nodes={movementConfirmDialog:dialog,movementConfirmTitle:{},movementConfirmDetail:{}};
  const ctx=vm.createContext({el:id=>nodes[id],plan:{departure:'EFHV',destination:'EFHN'},requestId:'TEST-FLIGHT'});
  const start=source.indexOf('    async function confirmMovement(');
  const end=source.indexOf('    async function recordMovement(',start);
  vm.runInContext(source.slice(start,end),ctx);
  const cancelled=vm.runInContext("confirmMovement('Record OUT','manual local time 10:05')",ctx);
  assert.equal(dialog.returnValue,'cancel');
  close();
  assert.equal(await cancelled,false);
  const confirmed=vm.runInContext("confirmMovement('Record OUT','manual local time 10:05')",ctx);
  dialog.returnValue='record';
  close();
  assert.equal(await confirmed,true);
  assert.equal(shown,2);
  assert.match(nodes.movementConfirmDetail.textContent,/EFHV.*EFHN.*TEST-FLIGHT/);
});

test('Home never treats movement records as proof of PIC release', () => {
  const ops=fs.readFileSync(new URL('../views/booking-ops.html',import.meta.url),'utf8');
  const start=ops.indexOf('      const heroReleased =');
  const end=ops.indexOf('      updateOpsGlobeContext(req);',start);
  for (const released of [false,true]) {
    const steps=['planning','released','airborne','arrived'].map(name=>({dataset:{heroPhase:name},classes:{},label:{}}));
    for(const step of steps) { step.classList={toggle:(name,value)=>{step.classes[name]=value;}}; step.querySelector=()=>step.label; }
    const ctx=vm.createContext({req:{},ofp:{actualOut:'0700Z/1000L',actualOff:'0710Z/1010L',actualOn:'0750Z/1050L',actualIn:'0755Z/1055L'},releaseClientState:()=>({released}),el:()=>({style:{setProperty:()=>{}}}),document:{querySelectorAll:()=>steps}});
    vm.runInContext(ops.slice(start,end),ctx);
    assert.equal(steps[1].classes.complete,released);
    assert.equal(steps[1].label.textContent,released ? 'Released' : 'Release open');
    assert.equal(steps[3].classes.complete,true);
  }
});

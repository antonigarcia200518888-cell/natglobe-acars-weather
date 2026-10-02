import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../views/operational-flight-plan.html', import.meta.url), 'utf8');
const markup = source.replace(/<script[\s\S]*?<\/script>/g, '');
const functionSource = (name, nextName) => source.slice(source.indexOf(`    function ${name}(`), source.indexOf(`    ${nextName}`));

test('all EFB inline scripts parse', () => {
  for (const file of ['operational-flight-plan.html', 'booking-ops.html']) {
    const html = fs.readFileSync(new URL(`../views/${file}`, import.meta.url), 'utf8');
    for (const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
  }
});

test('planner has one stylesheet, four stages, and no duplicate IDs or fields', () => {
  for (const attribute of ['id', 'data-field']) {
    const values = [...markup.matchAll(new RegExp(`\\s${attribute}="([^"]+)"`, 'g'))].map(m => m[1]);
    assert.equal(values.length, new Set(values).size, `duplicate ${attribute}`);
  }
  assert.equal([...markup.matchAll(/role="tab" data-workspace=/g)].length, 4);
  assert.equal([...markup.matchAll(/rel="stylesheet"/g)].length, 1);
  assert.doesNotMatch(source, /foreflight/i);
  for (const field of ['departure','destination','route','depRunway','arrRunway','estimatedEnrouteMinutes','fuelFlowGph','tripFuelGal','finalReserveFuelGal','crew1Lb','passengerWeightOverrideLb','releaseAccepted','actualOut','actualOff','actualOn','actualIn']) {
    assert.ok(markup.includes(`data-field="${field}"`), `missing ${field}`);
  }
  assert.match(source, /BRIEF:\['dispatchSection', 'releaseSection'\]/);
  assert.match(source, /workspaceOrder = \['SETUP', 'LOAD', 'BRIEF', 'FLY'\]/);
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
});

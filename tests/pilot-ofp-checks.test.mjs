import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../views/operational-flight-plan.html', import.meta.url), 'utf8');
const helperSource = fs.readFileSync(new URL('../public/pilot-ofp-checks.js', import.meta.url), 'utf8');
const context = vm.createContext({window:{}});
vm.runInContext(helperSource, context);
const checks = context.window.pilotOFPChecks;
const complete = {setupReady:true,routeReady:true,loadReady:true,reviewReady:true,performanceReady:true};

test('runway and alternate checks point to setup, not the route page', () => {
  const runway = checks.describeWarning('Enter valid departure and arrival runway identifiers (01–36, optional L/C/R).');
  assert.equal(runway.workspace, 'SETUP');
  assert.equal(runway.selector, '[data-field="depRunway"]');
  const alternate = checks.describeWarning('Alternate review remains open.');
  assert.equal(alternate.workspace, 'SETUP');
  assert.equal(alternate.selector, '[data-field="alternate"]');
  assert.equal(checks.describeWarning('Pilot route is not complete.').workspace, 'ROUTE');
});

test('every existing calculation warning has a real navigation target', () => {
  const calculation = source.slice(source.indexOf('    function calculations('),source.indexOf('    function setWorkspaceState('));
  const warnings = [...calculation.matchAll(/warnings\.push\('([^']+)'\)/g)].map(match=>match[1]);
  assert.ok(warnings.length >= 20);
  for (const warning of warnings) {
    const action = checks.describeWarning(warning);
    assert.notEqual(action.gate,'other',warning);
    const field = action.selector.match(/^\[data-field="([^"]+)"\]$/)?.[1];
    assert.ok(source.includes(field ? `data-field="${field}"` : `id="${action.selector.slice(1)}"`),warning);
  }
  assert.equal(checks.describeWarning('Zero fuel weight exceeds the locked 2650 LB limit.').selector,'#loadModeSelect');
  assert.equal(checks.describeWarning('Fuel exceeds the locked 288 LB / 48 USG limit.').selector,'[data-field="tripFuelGal"]');
});

test('planning checks preserve warnings and do not hide an incomplete gate', () => {
  assert.equal(checks.build([],complete).length,0);
  const fuel = checks.build([],{...complete,loadReady:false});
  assert.equal(fuel.length,1);
  assert.equal(fuel[0].selector,'[data-field="tripFuelGal"]');
  const warnings = ['Enter actual crew weights.','Enter actual crew weights.'];
  const result = checks.build(warnings,{...complete,loadReady:false,performanceReady:false});
  assert.equal(result.length,2);
  assert.equal(result[0].message,warnings[0]);
  assert.equal(result[1].gate,'performance');
  const ordered = checks.build(['Enter actual crew weights.','Alternate review remains open.'], {...complete,loadReady:false,routeReady:false});
  assert.equal(ordered[0].gate,'route');
  assert.equal(checks.build(['Unrecognized server-independent warning'],complete)[0].gate,'other');
});

test('unsaved or missing server checks never look current or released', () => {
  assert.equal(checks.serverState({ready:true,released:true},false,true).state,'stale');
  assert.equal(checks.serverState(null,true,false).state,'unavailable');
  assert.equal(checks.serverState({},true,false).state,'blocked');
  assert.equal(checks.serverState({ready:true},true,false).state,'ready');
  assert.match(checks.serverState({ready:true},true,false).title,/not released/);
  assert.equal(checks.serverState({released:true},true,true).state,'released');
});

test('server links route only known blockers and leave unknown text without dummy controls', () => {
  assert.equal(checks.serverAction('ASSIGN THE COMMANDER').module,'flights');
  assert.equal(checks.serverAction('ASSIGN THE COMMANDER').detailTab,'crew');
  assert.equal(checks.serverAction('WEATHER OPEN').detailTab,'release');
  assert.equal(checks.serverAction('AIRCRAFT OPEN').module,'aircraft');
  assert.equal(checks.serverAction('COMPLETE ROUTE AND RUNWAYS').workspace,'SETUP');
  for (const message of ['UNKNOWN OPEN','SAVE THE OFP DRAFT','toString','__proto__']) assert.equal(checks.serverAction(message),null);
  assert.match(source,/escapeHtml\(message\)/);
});

test('opening a check expands disclosures, focuses the invalid arrival runway and never edits it', () => {
  const calls = [];
  const details = {tagName:'DETAILS',open:false,parentElement:null};
  const label = {classList:{add:value=>calls.push(value)}};
  const target = {parentElement:details,closest:()=>label,matches:()=>true,scrollIntoView:()=>calls.push('scroll'),focus:()=>calls.push('focus')};
  const nodes = {checkReturnBar:{hidden:true},checkReturnLabel:{textContent:''}};
  const ctx = vm.createContext({
    document:{querySelector:selector=>{calls.push(selector); return target;}},
    window:{requestAnimationFrame:callback=>callback()},
    el:id=>nodes[id], inputFor:name=>({value:name==='depRunway'?'04':''}),
    setWorkspace:value=>calls.push(value), openPilotWorkspace:value=>calls.push(value)
  });
  const start=source.indexOf('    function openBriefingCheck(');
  vm.runInContext(source.slice(start,source.indexOf('    function renderBriefingDesk(',start)),ctx);
  ctx.check=checks.describeWarning('Enter valid departure and arrival runway identifiers (01–36, optional L/C/R).');
  vm.runInContext('openBriefingCheck(check)',ctx);
  assert.equal(calls[0],'SETUP');
  assert.equal(calls[1],'[data-field="arrRunway"]');
  assert.equal(details.open,true);
  assert.equal(nodes.checkReturnBar.hidden,false);
  assert.deepEqual(calls.slice(-2),['scroll','focus']);
  assert.match(source,/setWorkspace\('BRIEF'\);\s*el\('briefingDeskTitle'\)\.focus/);
});

test('briefing helper is versioned, cached and refreshed with the EFB shell', () => {
  const sw=fs.readFileSync(new URL('../public/pilot-sw.js',import.meta.url),'utf8');
  const version=sw.match(/PILOT_STYLESHEET_VERSION = '([^']+)'/)[1];
  assert.ok(source.includes(`/pilot-ofp-checks.js?v=${version}`));
  assert.match(sw,/`\/pilot-ofp-checks\.js\?v=\$\{PILOT_STYLESHEET_VERSION\}`/);
  assert.match(sw,/'\/pilot-ofp-checks\.js'\]\.includes\(url\.pathname\)/);
});

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const read=file=>fs.readFileSync(new URL('../'+file,import.meta.url),'utf8');
const context=vm.createContext({window:{}});
vm.runInContext(read('public/pilot-instruments.js'),context);
const charts=context.window.PilotInstruments;
const profile={forwardCgLimits:[{weightLbs:1400,cgIn:80},{weightLbs:2650,cgIn:87.3}],aftCgLimits:[{weightLbs:1400,cgIn:93},{weightLbs:2650,cgIn:93}]};

test('missing durations are not zero; a recorded zero remains distinct',()=>{
  const target={};
  charts.durations(target,[null,null,null,null]); assert.match(target.innerHTML,/No planned or recorded/);
  charts.durations(target,[40,null,55,0]); assert.match(target.innerHTML,/>—<\/text>/); assert.match(target.innerHTML,/>0<\/text>/);
  assert.equal((target.innerHTML.match(/<rect /g)||[]).length,2);
});
test('fuel chart uses only finite nonnegative values and skips identical DOM writes',()=>{
  let writes=0, html='';
  const target={set innerHTML(str){html=str;writes++;}};
  charts.fuel(target,[0,0,0,0]); assert.match(html,/No planned fuel/);
  charts.fuel(target,[1.5,8.2,9,0]); charts.fuel(target,[1.5,8.2,9,0]); assert.equal(writes,2);
  assert.match(html,/>8.2<\/text>/); charts.fuel(target,[NaN,-1,'<img src=x>',Infinity]); assert.doesNotMatch(html,/<img|NaN|Infinity/);
});
test('envelope plots approved straight edges and outliers without modifying the profile',()=>{
  const orig=structuredClone(profile), target={};
  charts.envelope(target,{profile,tow:2700,cg:94,gearUpCg:92,takeoffEnvelope:{valid:false},gearUpEnvelope:{valid:false}});
  assert.match(target.innerHTML,/<polygon points="/); assert.match(target.innerHTML,/94.00 in \(outside\)/);
  assert.match(target.innerHTML,/2700.0 lb/); assert.match(target.innerHTML,/cx="339.00" cy="31.00"/);
  assert.deepEqual(profile,orig); charts.limits(target,profile); assert.match(target.innerHTML,/87.30 IN \/ 2650 LB/);
  charts.envelope(target,{profile,tow:NaN}); assert.match(target.innerHTML,/unavailable/);
  charts.limits(target,{}); assert.match(target.innerHTML,/unavailable/);
});
test('all Pilot Ops entry points use the current offline design system and parse as JavaScript',()=>{
  const sw=read('public/pilot-sw.js'), version=sw.match(/PILOT_STYLESHEET_VERSION = '([^']+)'/)[1];
  for(const file of ['views/booking-ops.html','views/operational-flight-plan.html','views/pilot-login.html','public/pilot-offline.html']){
    const page=read(file); assert.ok(page.includes('/design-tokens.css?v='+version), file); assert.match(page,/viewport-fit=cover/);
    for(const [,source] of page.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)) new vm.Script(source,{filename:file});
  }
  for(const asset of ['design-tokens.css','pilot-instruments.js']) assert.ok(sw.includes('/'+asset+'?v='));
  const css=read('public/design-tokens.css'); for(const token of ['#00E676','#FFB300','#FF3B30','#0e203a','#101f3b','#7f8083'])assert.ok(css.includes(token));
});

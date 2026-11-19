import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createWeatherService} from '../lib/weather-service.js';
import {radarManifest,createRadarService,FMI_LAYER} from '../lib/fmi-radar.js';
const read=name=>fs.readFileSync(new URL('../'+name,import.meta.url),'utf8');
test('weather batches station requests and deduplicates in-flight/cached reports',async()=>{
  const calls=[];let now=100000;
  const client=createWeatherService({now:()=>now,batchMs:1,spacingMs:0,fetchImpl:async url=>{calls.push(url);return {ok:true,status:200,json:async()=>[{icaoId:'EFHK',rawOb:'EFHK METAR'},{icaoId:'EFTP',rawOb:'EFTP METAR'}]};}});
  const [first,shared,second]=await Promise.all([client.get('metar','EFHK'),client.get('metar','EFHK'),client.get('metar','EFTP')]);
  assert.equal(calls.length,1);assert.match(calls[0],/ids=EFHK,EFTP/);assert.equal(first.cache,'miss');assert.equal(shared.cache,'shared');assert.equal(second.raw,'EFTP METAR');
  assert.equal((await client.get('metar','EFHK')).cache,'hit');assert.equal(calls.length,1);
  now+=91000;await client.get('metar','EFHK');assert.equal(calls.length,2);
});
test('weather distinguishes provider failures, empty observations and invalid station input',async()=>{
  let mode=503,now=0;
  const client=createWeatherService({now:()=>now,batchMs:1,spacingMs:0,fetchImpl:async()=>({ok:mode===204,status:mode})});
  assert.equal((await client.get('taf','EFHK')).status,'error');
  mode=204;now+=11000;assert.equal((await client.get('taf','EFHK')).status,'missing');
  await assert.rejects(client.get('metar','EFH'),/four-letter/);
});
test('weather captures body/parse failures instead of calling them missing reports',async()=>{
  const client=createWeatherService({batchMs:1,spacingMs:0,fetchImpl:async()=>({ok:true,status:200,json:async()=>{throw new Error('truncated JSON');}})});
  assert.equal((await client.get('metar','EFHK')).error,'truncated JSON');
});
const xml=`<WMS_Capabilities><Capability><Layer><Layer><Name>${FMI_LAYER}</Name><Dimension name="time">2026-11-18T00:00:00Z/2026-11-18T12:00:00Z/PT5M</Dimension></Layer></Layer></Capability></WMS_Capabilities>`;
test('FMI frames are bounded observations and never future frames',()=>{
  const data=radarManifest(xml,Date.parse('2026-11-18T11:58:00Z'));
  assert.equal(data.frames.length,25);assert.equal(data.frames.at(-1).iso,'2026-11-18T11:55:00.000Z');
  assert.ok(data.frames.every(f=>f.kind==='observed'));assert.match(data.legend,/GetLegendGraphic/);assert.equal(data.license,'CC BY 4.0');
  assert.throws(()=>radarManifest('<WMS_Capabilities/>'),/no valid/);
});
test('FMI capabilities are cached and shared',async()=>{
  let calls=0;const get=createRadarService({now:()=>Date.parse('2026-11-18T12:00:00Z'),fetchImpl:async()=>{calls++;return {ok:true,text:async()=>xml};}});
  await Promise.all([get(),get()]);await get();assert.equal(calls,1);
});
test('new browser modules parse, weather panels are unique, and no RainViewer operational dependency remains',()=>{
  for(const name of ['pilot-weather-workspace.js','pilot-weather-radar.js','pilot-ofp-layout.js'])new vm.Script(read('public/'+name));
  const html=read('views/booking-ops.html');
  for(const id of ['weatherBriefingPanel','weatherAirportsPanel','weatherRadarPanel','weatherRoutePanel','weatherRequestsPanel','weatherStatus','weatherAirportGrid','refreshWeatherBtn'])assert.equal(html.split('id="'+id+'"').length,2,id);
  assert.doesNotMatch(read('server.js'),/rainviewer/i);
  assert.match(read('public/pilot-weather-radar.js'),/tileLayer\.wms/);
  assert.match(read('public/pilot-weather-workspace.js'),/controller\?\.abort/);
});

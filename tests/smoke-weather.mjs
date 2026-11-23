// Live-provider smoke test, explicitly against the isolated localhost server.
import assert from 'node:assert/strict';
const base=process.env.PILOT_QA_URL || 'http://localhost:3134';
assert.ok(['localhost','127.0.0.1'].includes(new URL(base).hostname));
const login=await fetch(base+'/api/pilot-login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code:'NGA-LOCAL-QA-ONLY'})});
assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
async function get(path){const start=performance.now(),response=await fetch(base+path,{headers:{Cookie:cookie},signal:AbortSignal.timeout(30000)});return {status:response.status,data:await response.json(),ms:Math.round(performance.now()-start)};}
assert.equal((await get('/api/booking-ops/weather-report?icao=EFH')).status,400);
for(let i=0;i<2;i++){
  const result=await get('/api/booking-ops/weather-report?icao=EFHK&product=summary');assert.equal(result.status,200);
  assert.ok(result.data.reports.some(r=>r.status==='ok'));assert.ok(result.data.observation.observedAt);
  assert.equal(result.data.efhvLocal,null);
  console.log(JSON.stringify({request:'EFHK summary',run:i+1,ms:result.ms,reports:result.data.reports.map(r=>({type:r.product,status:r.status,cache:r.cache}))}));
}
for(let i=0;i<2;i++){
  const result=await get('/api/booking-ops/weather?dep=EFHV&arr=EFHN&includeWindsAloft=false');assert.equal(result.status,200);
  assert.equal(result.data.windsAloft.departure,null);assert.equal(result.data.windsAloft.arrival,null);
  assert.ok(result.data.efhvLocal?.observedAt,'EFHV site sensor timestamp should be pulled in');
  assert.ok(result.data.efhvLocal.forecast?.validAt,'EFHV site forecast should be parsed separately');
  assert.doesNotMatch(result.data.reportText,/efhv\.fi/i,'supplemental data must not be presented as an official report');
  console.log(JSON.stringify({request:'EFHV–EFHN briefing without winds',run:i+1,ms:result.ms,modes:[result.data.departure.mode,result.data.arrival.mode]}));
}
const local=await get('/api/booking-ops/weather-report?icao=EFHV&product=summary');
assert.equal(local.status,200);assert.ok(local.data.efhvLocal?.forecast);assert.ok(local.data.reports.every(r=>r.icao==='EFHV'));
console.log(JSON.stringify({request:'EFHV local supplement',measuredAt:local.data.efhvLocal.observedAt,forecastAt:local.data.efhvLocal.forecast.validAt,windGraphic:Boolean(local.data.efhvLocal.windChart)}));
const radar=await get('/api/booking-ops/weather-radar');assert.equal(radar.status,200);assert.equal(radar.data.provider,'Finnish Meteorological Institute');
assert.equal(radar.data.frames.length,25);assert.ok(radar.data.frames.every(f=>f.time*1000<=Date.now()));
console.log(JSON.stringify({radar:'FMI',frames:radar.data.frames.length,latest:radar.data.frames.at(-1).iso,ms:radar.ms}));

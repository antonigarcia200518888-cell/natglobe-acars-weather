import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import express from 'express';
import {parseAviationImport,validUtc} from '../lib/aviation-import.js';
import {planPatch,parseOffThread,suggestFlights,mountAviationImports,IMPORT_SCHEMA,revision} from '../lib/aviation-import-routes.js';
const parse=(s,kind='plan',name='route.txt')=>parseAviationImport(Buffer.from(s),{kind,name});
const csv='Timestamp,Latitude,Longitude,Altitude,Speed,Course\n1794916800,60.32,24.96,2000,100,270\n1794916860,60.33,24.98,2100,101,271';
const route='Departure: EFHK\nDestination: EETN\nRoute: EFHK DCT EETN\nREG: OH-PMK\nRules: IFR';
test('imported UTC survives local midnight and both occurrences of a daylight-saving fold',()=>{
  const server=readFileSync(new URL('../server.js',import.meta.url),'utf8');
  const code=server.slice(server.indexOf('function ofpTimeZoneParts('),server.indexOf('function operationalFlightPlanDefaults('));
  const ctx=vm.createContext({bookingAirportTimeZone:()=> 'Europe/Helsinki'});vm.runInContext(code,ctx);
  for(const utc of ['2026-11-17T23:30:00Z','2026-10-25T00:30:00Z','2026-10-25T01:30:00Z']){
    ctx.utc=utc;
    const p=vm.runInContext("operationalImportSchedule(utc,'EFHK')",ctx);ctx.p=p;
    const result=vm.runInContext('ofpSmartSchedule(p.dateUtc,p.scheduledOutLocal,p.departureTimeZone,40,p.departureTimeOccurrence)',ctx);
    assert.equal(result.out.instant,new Date(utc).toISOString());
    if(utc.includes('11-17')){assert.equal(p.dateUtc,'2026-11-18');assert.equal(p.scheduledOutLocal,'01:30');}
    else assert.equal(p.departureTimeOccurrence,utc.includes('00:30')?'EARLIER':'LATER');
  }
});
test('route text, ICAO identifiers and planned UTC are explicit, never performance defaults',()=>{
  const p=parse(route);assert.equal(p.fields.departure,'EFHK');assert.equal(p.fields.aircraftRegistration,'OH-PMK');assert.equal(p.fields.fuelFlowGph,undefined);assert.deepEqual(p.missing,['plannedDepartureUtc']);
  const f=parse('(FPL-NGA123-IG-PA28/L-S/C-EFHK1200-N0110F060 DCT-EETN0040 EFHK-DOF/261117 REG/OHPMK)');
  assert.equal(f.fields.callsign,'NGA123');assert.equal(f.fields.aircraftRegistration,'OHPMK');assert.equal(f.fields.plannedDepartureUtc,'2026-11-17T12:00:00.000Z');
  const withoutReg=parse('(FPL-NGA123-IG-PA28/L-S/C-EFHK1200-N0110F060 DCT-EETN0040 EFHK-DOF/261117)');assert.equal(withoutReg.fields.aircraftRegistration,undefined);
  for(const invalid of ['2026-02-30T12:00Z','2026-11-17T24:10Z','2026-11-17T12:00'])assert.equal(validUtc(invalid),false);
  assert.equal(validUtc('2026-11-17T00:00Z'),true);
});
test('unverified aircraft never enter the OH-PMK mass/balance calculation path',()=>{
  const server=readFileSync(new URL('../server.js',import.meta.url),'utf8');
  const code=server.slice(server.indexOf('function normalizeOperationalFlightPlan('),server.indexOf('function operationalPlanDependencyFingerprint('));
  let calculations=0;
  const ctx=vm.createContext({
    operationalFlightPlanDefaults:()=>({}),normalizeBookingText:(v,max)=>String(v??'').slice(0,max),operationalPlanNumber:(v,fallback)=>v==null?fallback:Number(v),
    normalizeOperationalRouteLegs:v=>v||[],normalizeOperationalPerformanceSnapshot:v=>v,normalizeOperationalReleaseChecks:v=>v,
    AIRCRAFT_WEIGHT_BALANCE_PROFILE:{stations:{cockpit:{},rearSeats:{},fuel:{},baggage:{}},forwardCgLimits:[{}],aftCgLimits:[{}]},
    bookingAirports:[],ofpLocalFlightCandidates:()=>null,ofpSmartSchedule:()=>null,
    operationalFlightPlanCalculations:()=>{calculations++;return {takeoffWeightLb:1800};}
  });vm.runInContext(code,ctx);
  const unknown=vm.runInContext("normalizeOperationalFlightPlan({aircraftRegistration:'N12345'},{id:'TEST'})",ctx);
  assert.equal(calculations,0);assert.equal(unknown.aircraftProfileVerified,false);assert.equal(unknown.takeoffWeightLb,null);assert.equal(unknown.bemLb,null);
  const known=vm.runInContext("normalizeOperationalFlightPlan({aircraftRegistration:'OH-PMK'},{id:'TEST'})",ctx);
  assert.equal(calculations,1);assert.equal(known.aircraftProfileVerified,true);
});
test('Garmin FPL resolves ordered composite waypoint references; GPX routes stay distinct from tracks',()=>{
  const waypoint=id=>`<waypoint><identifier>${id}</identifier><type>AIRPORT</type><country-code>FI</country-code><lat>60</lat><lon>24</lon></waypoint>`;
  const ref=id=>`<route-point><waypoint-identifier>${id}</waypoint-identifier><waypoint-type>AIRPORT</waypoint-type><waypoint-country-code>FI</waypoint-country-code></route-point>`;
  const xml=`<flight-plan xmlns="http://www8.garmin.com/xmlschemas/FlightPlan/v1"><waypoint-table>${waypoint('EFHK')}${waypoint('EFHV')}</waypoint-table><route>${ref('EFHV')}${ref('EFHK')}</route></flight-plan>`;
  assert.deepEqual(parse(xml,'plan','test.fpl').waypoints.map(w=>w.ident),['EFHV','EFHK']);assert.throws(()=>parse(xml.replace(ref('EFHK'),ref('ZZZZ'))),/missing or ambiguous/);
  assert.throws(()=>parse('<gpx><trk/></gpx>'),/GPX route/);
  assert.equal(parse('<gpx><rte><rtept lat="60" lon="24"><name>EFHK</name></rtept><rtept lat="59" lon="24"><name>EETN</name></rtept></rte></gpx>').fields.destination,'EETN');
});
test('ForeFlight CSV uses Unix seconds, GPS feet and knots without deriving absent data',()=>{
  const t=parse(csv,'track','flight.csv');assert.equal(t.summary.durationSeconds,60);assert.equal(t.points[0].time,new Date(1794916800000).toISOString());assert.equal(t.points[0].altitudeFt,2000);assert.equal(t.points[0].speedKt,100);assert.equal(t.summary.replayAvailable,true);
  assert.equal(parse(csv.replaceAll('1794916860','1794916800'),'track').summary.replayAvailable,false);
  assert.throws(()=>parse(csv.replace('60.32','91'),'track'),/latitude/);
  assert.throws(()=>parse(csv.replace('1794916800','1794916800000'),'track'),/timestamp/);
});
test('KML and GPX preserve segments, convert metres, and do not invent timing or absolute altitude',()=>{
  const kml='<kml xmlns:gx="http://www.google.com/kml/ext/2.2"><Placemark><gx:Track><altitudeMode>absolute</altitudeMode><when>2026-11-17T12:00:00Z</when><when>2026-11-17T12:01:00Z</when><gx:coord>24 60 304.8</gx:coord><gx:coord>24.1 60.1 609.6</gx:coord></gx:Track></Placemark></kml>';
  assert.equal(parse(kml,'track').points[0].altitudeFt,1000);assert.equal(parse(kml.replace('absolute','relativeToGround'),'track').points[0].altitudeFt,null);
  const line='<kml><Placemark><LineString><coordinates>24,60,100 24.1,60.1,200</coordinates></LineString></Placemark></kml>';
  const t=parse(line,'track');assert.equal(t.summary.replayAvailable,false);assert.equal(t.summary.durationSeconds,null);
  const gpx='<gpx><trk><trkseg><trkpt lat="60" lon="24"><ele>304.8</ele><time>2026-11-17T12:00:00Z</time></trkpt></trkseg><trkseg><trkpt lat="61" lon="25"><time>2026-11-17T12:05:00Z</time></trkpt></trkseg></trk></gpx>';
  const g=parse(gpx,'track');assert.equal(g.summary.gaps,1);assert.equal(g.summary.distanceNm,0);assert.equal(g.points[1].altitudeFt,null);
});
test('hostile, oversized, compressed and malformed sources are rejected',()=>{
  for(const s of ['<!DOCTYPE x [<!ENTITY a SYSTEM "file:///etc/passwd">]><x>&a;</x>','<gpx><trk></gpx>','PK\0\0','x'.repeat(8388609)])assert.throws(()=>parse(s));
  assert.throws(()=>parse('(FPL-X-VG-PA28/L-S/C-EFHK1200-N0110F060 DCT-EETN0040-DOF/260230)'),/calendar/);
  assert.throws(()=>parse('(FPL-X-VG-PA28/L-S/C-EFHK2400-N0110F060 DCT-EETN0040-DOF/261117)'),/timestamp clock/);
});
test('review patches protect unchecked manual values and invalidate changed-route calculations',()=>{
  const p=parse(route),old={departure:'EFHV',route:'MANUAL',tripFuelGal:9};
  assert.deepEqual(planPatch(p,{fields:['aircraftRegistration']},old),{aircraftRegistration:'OH-PMK'});
  const change=planPatch(p,{fields:['departure','route']},old);assert.equal(change.departure,'EFHK');assert.equal(change.tripFuelGal,0);assert.equal(change.depRunway,'');assert.equal(change.releaseSnapshot,null);
  assert.throws(()=>planPatch(p,{fields:['plannedDepartureUtc'],corrections:{plannedDepartureUtc:'2026-02-30T12:00Z'}}),/UTC/);
});
test('worker parsing keeps extraction off the server event loop and matching only suggests',async()=>{
  const p=await parseOffThread(Buffer.from(csv),{kind:'track',name:'test.csv'});assert.equal(p.points.length,2);
  const records=[{id:'A',dep:'EFHK',arr:'EETN',operationalFlightPlan:{dateUtc:p.summary.start.time.slice(0,10)}}];
  const matches=suggestFlights(p,records,[]);assert.equal(matches[0].id,'A');assert.equal(records[0].trackCount,undefined);
});
test('replay interpolation crosses the dateline briefly and never bridges a recording gap',()=>{
  const ctx={window:{}};vm.runInNewContext(readFileSync(new URL('../public/pilot-replay.js',import.meta.url),'utf8'),ctx);
  const {prepare,sample}=ctx.window.PilotTrackMath;
  const t={points:[{lat:0,lon:179,time:'2026-11-17T12:00:00Z',altitudeFt:null,speedKt:null,segment:0},{lat:1,lon:-179,time:'2026-11-17T12:01:00Z',altitudeFt:null,speedKt:null,segment:0},{lat:3,lon:30,time:'2026-11-17T12:10:00Z',segment:1}]};
  const p=prepare(t);assert.equal(p.geometry.features.length,1);const middle=sample(p.points,Date.parse('2026-11-17T12:00:30Z'));assert.equal(Math.abs(middle.lon),180);assert.equal(middle.altitudeFt,null);
  const gap=sample(p.points,Date.parse('2026-11-17T12:05:00Z'));assert.equal(gap.gap,true);assert.equal(gap.lon,-179);
});
test('imports persist original bytes, preserve manual fields, reject stale revisions and roll back failures',{skip:!process.env.PILOT_TEST_PGLITE},async t=>{
  const {PGlite}=await import(process.env.PILOT_TEST_PGLITE);const db=new PGlite();t.after(()=>db.close());
  await db.exec('CREATE TABLE booking_requests(id TEXT PRIMARY KEY,payload JSONB,created_at TIMESTAMPTZ);'+IMPORT_SCHEMA);
  const records=[{id:'A',dep:'EFHV',arr:'EETN',management:{category:'private',training:{notes:'retain'}},operationalFlightPlan:{departure:'EFHV',destination:'EETN',aircraftRegistration:'OH-PMK',route:'MANUAL ROUTE',fuelFlowGph:8}}];
  await db.query('INSERT INTO booking_requests VALUES($1,$2,now())',['A',JSON.stringify(records[0])]);
  const pool={query:async(...args)=>db.query(...args),connect:async()=>({...pool,release(){}})};
  const app=express();app.use(express.json());mountAviationImports(app,{auth:(r,s,n)=>r.get('X-Test')?n():s.sendStatus(401),crew:(r,s)=>r.get('X-Test')==='crew'||(s.sendStatus(403),false),raw:express.raw({type:'application/octet-stream',limit:'8mb'}),ready:Promise.resolve(),pool:()=>pool,records:()=>records,createFile:()=>({id:'B',createdAt:new Date().toISOString()}),defaults:()=>({}),normalize:p=>p,airports:()=>[]});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>new Promise(r=>server.close(r)));
  const base='http://127.0.0.1:'+server.address().port+'/api/booking-ops/imports';
  const call=(path,opt={})=>fetch(base+path,{...opt,headers:{'X-Test':'crew',...opt.headers}});
  const post=path=>call(path,{method:'POST',headers:{'Content-Type':'application/octet-stream'},body:route});
  assert.equal((await fetch(base+'/files/A')).status,401);
  const preview=await (await post('/preview?kind=plan')).json();
  const options={target:'A',revision:preview.revisions.A,category:'private',sha256:preview.parsed.sha256,fields:['alternate'],corrections:{alternate:'EFHK'}};
  const confirm=()=>call('/confirm?kind=plan',{method:'POST',headers:{'Content-Type':'application/octet-stream','X-NGA-Import':JSON.stringify(options)},body:route});
  const saved=await confirm();assert.equal(saved.status,201,await saved.clone().text());const body=await saved.json();
  assert.equal(body.request.operationalFlightPlan.route,'MANUAL ROUTE');assert.equal(body.request.operationalFlightPlan.fuelFlowGph,8);assert.equal(body.request.management.training.notes,'retain');
  assert.equal((await confirm()).status,409);options.revision=revision(records[0]);assert.equal((await confirm()).status,200);
  const original=await call('/'+body.importId+'?download=1');assert.equal(await original.text(),route);assert.equal(original.headers.get('cache-control'),'private, no-store');
  const tracks=await call('/preview?kind=track&name=test.csv',{method:'POST',headers:{'Content-Type':'application/octet-stream'},body:csv});const tp=await tracks.json();
  const old=JSON.stringify(records[0].operationalFlightPlan),source=JSON.stringify(records[0].importProvenance);
  const trackSaved=await call('/confirm?kind=track&name=test.csv',{method:'POST',headers:{'Content-Type':'application/octet-stream','X-NGA-Import':JSON.stringify({...options,sha256:tp.parsed.sha256,revision:tp.revisions.A})},body:csv});assert.equal(trackSaved.status,201,await trackSaved.clone().text());
  assert.equal(JSON.stringify(records[0].operationalFlightPlan),old);assert.equal(JSON.stringify(records[0].importProvenance),source);
  assert.equal(records[0].trackCount,1);
  assert.equal((await (await call('/files/A')).json()).imports.length,2);
  assert.equal(body.request.importProvenance.fields.alternate,'EFHK');
  assert.equal(body.request.importProvenance.origins.alternate,'Pilot confirmed / corrected');
  assert.equal(body.request.importProvenance.fields.route,undefined);
  assert.equal((await call('/files/A',{headers:{'X-Test':'passenger'}})).status,403);
  assert.equal((await call('/files/A',{headers:{'Sec-Fetch-Site':'cross-site'}})).status,403);
  const invalid=await call('/confirm?kind=plan',{method:'POST',headers:{'Content-Type':'application/octet-stream','X-NGA-Import':'null'},body:route});assert.equal(invalid.status,400);

  // A fresh source for creation; inject a storage connection failure and ensure
  // the factory's temporary ID reservation cannot leak into the live list.
  const newRoute=route+'\nALTN: EFHV',p=parse(newRoute);
  const newOptions={category:'training',sha256:p.sha256,fields:['departure','destination','route','aircraftRegistration','flightRules','plannedDepartureUtc'],corrections:{plannedDepartureUtc:'2026-11-18T10:00Z'}};
  const create=()=>call('/confirm?kind=plan',{method:'POST',headers:{'Content-Type':'application/octet-stream','X-NGA-Import':JSON.stringify(newOptions)},body:newRoute});
  const connect=pool.connect;pool.connect=async()=>{throw new Error('Injected connection failure');};
  assert.equal((await create()).status,503);assert.equal(records.length,1);pool.connect=connect;
  const created=await create();assert.equal(created.status,201,await created.clone().text());
  const flight=(await created.json()).request;
  assert.equal(flight.operationalFlightPlan.dateUtc,'2026-11-18');
  assert.equal(flight.operationalFlightPlan.cruiseSpeedKt,0);
  assert.equal(flight.operationalFlightPlan.fuelFlowGph,0);
  assert.equal(flight.operationalFlightPlan.importReviewed,false);
  assert.equal((await db.query('SELECT COUNT(*)::int AS n FROM booking_requests')).rows[0].n,2);
});

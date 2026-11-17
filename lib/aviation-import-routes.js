import {Worker} from 'node:worker_threads';
import {randomUUID,createHash} from 'node:crypto';
import {distanceNm,validUtc} from './aviation-import.js';

export const IMPORT_SCHEMA=`CREATE TABLE IF NOT EXISTS pilot_flight_imports (
 id UUID PRIMARY KEY,flight_id TEXT NOT NULL REFERENCES booking_requests(id) ON DELETE CASCADE,
 kind TEXT NOT NULL CHECK(kind IN ('plan','track')),sha256 TEXT NOT NULL,
 filename TEXT NOT NULL,original BYTEA NOT NULL CHECK(octet_length(original)<=8388608),
 parsed JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(flight_id,kind,sha256)
);
CREATE INDEX IF NOT EXISTS pilot_flight_imports_source_idx ON pilot_flight_imports(kind,sha256)`;
const stable=value=>Array.isArray(value)?value.map(stable):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(k=>[k,stable(value[k])])):value;
export const revision=file=>createHash('sha256').update(JSON.stringify(stable([file.operationalFlightPlan,file.management]))).digest('hex');
const error=(status,message)=>Object.assign(new Error(message),{status});
let workers=0;
export async function parseOffThread(bytes,options){
  if(workers>=2)throw error(429,'Another import is being processed. Retry shortly.');
  workers++;
  return new Promise((resolve,reject)=>{
    let worker;
    try{worker=new Worker(new URL('./aviation-import-worker.js',import.meta.url),{workerData:{bytes,options},resourceLimits:{maxOldGenerationSizeMb:128}});}catch(e){workers--;reject(e);return;}
    let done=false;const finish=(e,result)=>{if(done)return;done=true;workers--;clearTimeout(timer);worker.terminate();e?reject(e):resolve(result);};
    const timer=setTimeout(()=>finish(error(422,'File parsing exceeded the time limit. Try a smaller export.')),10000);
    worker.once('message',m=>finish(m.error?error(422,m.error):null,m.result));worker.once('error',()=>finish(error(422,'The file could not be parsed safely.')));
    worker.once('exit',code=>{if(!done)finish(error(422,'The import worker stopped ('+code+').'));});
  });
}
export function suggestFlights(parsed,records,airports){
  return records.map(file=>{
    const plan=file.operationalFlightPlan||{},dep=plan.departure||file.dep,arr=plan.destination||file.arr;let score=0,reasons=[];
    if(parsed.kind==='plan'){
      if(dep===parsed.fields.departure&&arr===parsed.fields.destination){score+=3;reasons.push('Same airport pair');}
      if(parsed.fields.aircraftRegistration && plan.aircraftRegistration===parsed.fields.aircraftRegistration){score++;reasons.push('Same registration');}
    }else{
      const {start,end}=parsed.summary,a=airports.find(p=>p.icao===dep),b=airports.find(p=>p.icao===arr);
      if(a&&b&&distanceNm(start,a)<5&&distanceNm(end,b)<5){score+=3;reasons.push('Track endpoints within 5 NM of planned airports');}
      if(start.time?.slice(0,10)===plan.dateUtc){score+=2;reasons.push('Same OFP UTC date');}
    }
    return {id:file.id,route:dep+' → '+arr,date:file.requestDate,aircraft:plan.aircraftRegistration||file.aircraft,score,reasons,revision:revision(file)};
  }).filter(r=>r.score>0).sort((a,b)=>b.score-a.score).slice(0,8);
}
const fields=['departure','destination','alternate','route','flightRules','cruiseFlightLevel','aircraftRegistration','aircraftCode','callsign','estimatedEnrouteMinutes','plannedDepartureUtc'];
export function planPatch(parsed,options,previous={}){
  const approved=new Set(options.fields || []),patch={};
  for(const key of fields){
    if(!approved.has(key))continue;
    const value=options.corrections?.[key] ?? parsed.fields[key];
    if(value==null || value==='')continue;
    if(['departure','destination','alternate'].includes(key)&&!(/^[A-Z]{4}$/).test(value))throw error(422,'Four-letter ICAO required: '+key);
    if(key==='route'&&(typeof value!=='string'||value.length>240||!value.trim()||value.trim().split(/\s+/).length>80||!/^[A-Z0-9 ./,+-]+$/.test(value)))throw error(422,'Route needs 1–240 supported characters and no more than 80 elements.');
    if(key==='flightRules'&&!['IFR','VFR'].includes(value))throw error(422,'Confirm IFR or VFR.');
    if(key==='plannedDepartureUtc'&&!validUtc(value))throw error(422,'Confirm the full planned departure in UTC.');
    if(['aircraftRegistration','aircraftCode','cruiseFlightLevel','callsign'].includes(key)&&(typeof value!=='string'||value.length>16||!/^[A-Z0-9 -]+$/.test(value)))throw error(422,'Invalid '+key);
    if(key==='estimatedEnrouteMinutes'&&(!Number.isInteger(value)||value<0||value>1440))throw error(422,'Invalid EET.');
    patch[key]=value;
  }
  if(patch.plannedDepartureUtc){patch.dateUtc=patch.plannedDepartureUtc.slice(0,10);patch.scheduledOutUtc=patch.plannedDepartureUtc.slice(11,16);patch.scheduledOutLocal=patch.scheduledOutUtc;patch.departureTimeZone='UTC';}
  if(patch.flightRules)patch.routeMode=patch.flightRules;
  if(patch.route){
    patch.routeLegs=patch.route.trim().split(/\s+/).map(ident=>({ident,type:ident==='DCT'?'DCT':'IMPORTED',source:parsed.format+' / VERIFY',name:''}));
  }
  // Changed routes invalidate previously calculated route-dependent values.
  if(['route','departure','destination','alternate'].some(k=>patch[k]!=null&&patch[k]!==previous[k])){
    Object.assign(patch,{distanceNm:0,tripFuelGal:0,blockFuelGal:0,performanceSnapshot:null,releaseSnapshot:null,releaseAccepted:false,importReviewed:false});
    if(patch.departure&&patch.departure!==previous.departure)Object.assign(patch,{depRunway:'',departureProcedure:''});
    if(patch.destination&&patch.destination!==previous.destination)Object.assign(patch,{arrRunway:'',arrivalProcedure:'',approachProcedure:''});
  }
  return patch;
}
export function mountAviationImports(app,{auth,crew,raw,ready,pool,records,createFile,defaults,normalize,airports,schedule=(when)=>({dateUtc:when.slice(0,10),scheduledOutLocal:when.slice(11,16),scheduledOutUtc:when.slice(11,16),departureTimeZone:'UTC'})}){
  const base='/api/booking-ops/imports';
  const wrap=fn=>async(req,res)=>{try{await fn(req,res);}catch(e){if(!e.status)console.error('Import:',e.code||e.message);res.status(e.status||503).json({error:e.status?e.message:'Import storage unavailable. Nothing was confirmed.'});}};
  const guard=(req,res,next)=>{if(!crew(req,res))return;res.set('Cache-Control','private, no-store');if(req.get('Sec-Fetch-Site')==='cross-site')return res.sendStatus(403);Promise.resolve(ready).then(()=>next(),()=>res.status(503).json({error:'Flight storage is not ready.'}));};
  app.use(base,auth,guard);
  const parse=req=>parseOffThread(req.body,{kind:req.query.kind,name:String(req.query.name||'route.txt')});
  app.post(base+'/preview',raw,wrap(async(req,res)=>{
    const parsed=await parse(req),catalog=await airports();
    if(parsed.kind==='plan'){
      parsed.airports=['departure','destination','alternate'].flatMap(key=>parsed.fields[key]?[{field:key,icao:parsed.fields[key],known:catalog.some(a=>a.icao===parsed.fields[key])}]:[]);
      const unknown=parsed.airports.filter(a=>!a.known).map(a=>a.icao);
      if(unknown.length)parsed.warnings.push('Not in the NGA airport catalog: '+unknown.join(', ')+'. Verify these identifiers and airport data before planning.');
      parsed.warnings.push('Fuel, mass/balance and aircraft performance are not imported or certified by this file. Changing a route clears old route calculations; review fuel and performance again.');
      parsed.warnings.push('The map shows an endpoint overview, not a decoded airway/procedure path. Original route coordinates are retained in the private source file.');
    }
    let duplicates=[];
    if(pool()){const q=await pool().query('SELECT flight_id,id FROM pilot_flight_imports WHERE sha256=$1 AND kind=$2',[parsed.sha256,parsed.kind]);duplicates=q.rows;}
    res.json({parsed,matches:suggestFlights(parsed,records(),catalog),revisions:Object.fromEntries(records().map(f=>[f.id,revision(f)])),duplicates,storageAvailable:Boolean(pool())});
  }));
  app.get(base+'/files/:flightId',wrap(async(req,res)=>{
    if(!pool())throw error(503,'Persistent import storage is offline.');
    if(!records().some(f=>f.id===req.params.flightId))throw error(404,'Flight not found.');
    const q=await pool().query("SELECT id,kind,filename,created_at,parsed->'summary' AS summary FROM pilot_flight_imports WHERE flight_id=$1 ORDER BY created_at DESC",[req.params.flightId]);res.json({imports:q.rows});
  }));
  app.get(base+'/:id',wrap(async(req,res)=>{
    if(!pool())throw error(503,'Persistent import storage is offline.');
    if(!/^[\da-f-]{36}$/i.test(req.params.id))throw error(404,'Import not found.');
    const q=await pool().query('SELECT flight_id,filename,original,parsed FROM pilot_flight_imports WHERE id=$1',[req.params.id]),row=q.rows[0];
    if(!row || !records().some(f=>f.id===row.flight_id))throw error(404,'Import not found.');
if(req.query.download==='1'){res.set({'Content-Type':'application/octet-stream','X-Content-Type-Options':'nosniff','Content-Disposition':"attachment; filename*=UTF-8''"+encodeURIComponent(row.filename)}).send(Buffer.from(row.original));return;}
    res.json({flightId:row.flight_id,parsed:row.parsed});
  }));
  app.post(base+'/confirm',raw,wrap(async(req,res)=>{
    if(!pool())throw error(503,'Persistent storage is required. Review is available, but no import has been saved.');
    let options;try{options=JSON.parse(req.get('X-NGA-Import')||'{}');}catch{throw error(400,'Invalid import selection.');}
    if(!options||typeof options!=='object'||Array.isArray(options)||options.fields&&!Array.isArray(options.fields)||options.corrections&&(typeof options.corrections!=='object'||Array.isArray(options.corrections)))throw error(400,'Invalid import selection.');
    if(!['operational','private','training'].includes(options.category))throw error(422,'Choose a flight category.');
    const parsed=await parse(req);
    if(options.sha256!==parsed.sha256)throw error(409,'The uploaded file changed. Review it again.');
    const existing=options.target && records().find(f=>f.id===options.target);
    if(options.target&&!existing)throw error(404,'Selected flight no longer exists.');
    if(existing && options.revision!==revision(existing))throw error(409,'Flight changed since review. Reopen the review before applying changes.');
    if(existing && parsed.kind==='plan' && existing.operationalFlightPlan?.releaseSnapshot)throw error(409,'This OFP has a release record. Import as new to preserve the released document.');
    const duplicates=await pool().query('SELECT flight_id,id FROM pilot_flight_imports WHERE sha256=$1 AND kind=$2',[parsed.sha256,parsed.kind]);
    const same=duplicates.rows.find(r=>r.flight_id===existing?.id);if(same){res.json({request:existing,importId:same.id,duplicate:true});return;}
    if(duplicates.rows.length&&!options.allowDuplicate)throw error(409,'This file is already saved. Choose its existing flight or explicitly confirm another association.');
    const patch=parsed.kind==='plan'?planPatch(parsed,options,existing?.operationalFlightPlan):{};
    const c=options.corrections || {};
    if(!existing){
      if(!/^[A-Z]{4}$/.test(patch.departure||c.departure)||!/^[A-Z]{4}$/.test(patch.destination||c.destination))throw error(422,'Confirm departure and destination; track endpoints are not assumed to be airports.');
      if(!validUtc(c.plannedDepartureUtc||patch.plannedDepartureUtc))throw error(422,'Enter an explicit planned UTC date and time.');
      if(!/^[A-Z0-9-]{2,16}$/.test(patch.aircraftRegistration||c.aircraftRegistration||''))throw error(422,'Confirm aircraft registration.');
      if(!['IFR','VFR'].includes(patch.flightRules||c.flightRules))throw error(422,'Confirm IFR or VFR.');
    }
    const when=patch.plannedDepartureUtc||(!existing?c.plannedDepartureUtc:null);
    const local=when?schedule(when,patch.departure||existing?.operationalFlightPlan?.departure||c.departure):{};
    if(when&&parsed.kind==='plan')Object.assign(patch,local);
    const candidate=existing || createFile(req,{dep:patch.departure||c.departure,arr:patch.destination||c.destination,route:patch.route,category:options.category,requestDate:local.dateUtc,requestTime:local.scheduledOutLocal,flightRules:patch.flightRules||c.flightRules,title:'Imported flight'});
    if(!existing){candidate.aircraft=patch.aircraftRegistration||c.aircraftRegistration;if(parsed.kind==='track')candidate.route='';}
    if(!existing&&parsed.kind==='plan'&&!patch.route)throw error(422,'Confirm the planned route. No direct route is inferred.');
    if(!existing)records().push(candidate); // Reserve the existing factory reference until the transaction settles.
    let client;
    try{
      client=await pool().connect();
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',[parsed.kind+parsed.sha256]);
      const race=await client.query('SELECT flight_id FROM pilot_flight_imports WHERE sha256=$1 AND kind=$2',[parsed.sha256,parsed.kind]);
      if(race.rows.some(r=>r.flight_id===candidate.id)||race.rows.length&&!options.allowDuplicate)throw error(409,'This source has already been saved. Reopen review to select its existing flight.');
      if(existing){const q=await client.query('SELECT payload FROM booking_requests WHERE id=$1 FOR UPDATE',[existing.id]);if(!q.rows[0]||revision(q.rows[0].payload)!==options.revision)throw error(409,'Flight changed on the server. Review again.');}
      const count=await client.query("SELECT COUNT(*)::int AS n, COUNT(*) FILTER (WHERE kind='track')::int AS tracks FROM pilot_flight_imports WHERE flight_id=$1",[candidate.id]);if(count.rows[0].n>=12)throw error(409,'This flight already has twelve imported source files.');
const previous=existing?.operationalFlightPlan || {...defaults(candidate),aircraftRegistration:patch.aircraftRegistration||c.aircraftRegistration,aircraftCode:patch.aircraftCode||((patch.aircraftRegistration||c.aircraftRegistration)==='OH-PMK'?'PA28':'UNVERIFIED'),aircraftModel:(patch.aircraftRegistration||c.aircraftRegistration)==='OH-PMK'?defaults(candidate).aircraftModel:'UNVERIFIED PROFILE',flightRules:patch.flightRules||c.flightRules,routeMode:patch.flightRules||c.flightRules,cruiseFlightLevel:'',cruiseSpeedKt:0,fuelFlowGph:0,distanceNm:0,estimatedEnrouteMinutes:0,taxiFuelGal:0,tripFuelGal:0,contingencyFuelGal:0,finalReserveFuelGal:0,blockFuelGal:0,releaseSnapshot:null,releaseAccepted:false};
      const plan=parsed.kind==='plan'?normalize({...previous,...patch,importReviewed:false,releaseAccepted:false,routeDataSource:parsed.format+' / PILOT REVIEW REQUIRED',releaseSnapshot:null},candidate):existing?.operationalFlightPlan || normalize({...previous,...local,route:'',routeLegs:[],importReviewed:false},candidate);
      const accepted=Object.fromEntries(fields.filter(k=>patch[k]!=null).map(k=>[k,patch[k]]));
      const origins=Object.fromEntries(Object.entries(accepted).map(([k,v])=>[k,v===parsed.fields?.[k]?'Imported':'Pilot confirmed / corrected']));
      const source=parsed.kind==='track'?candidate.importProvenance||null:{source:'File import',format:parsed.format,sha256:parsed.sha256,importedAt:new Date().toISOString(),fields:accepted,origins,reviewRequired:true};
      const trackCount=count.rows[0].tracks+(parsed.kind==='track'?1:0);
      const management={...candidate.management,category:options.category};
      const saved={...candidate,operationalFlightPlan:plan,management,importProvenance:source,trackCount};
      if(existing){
        await client.query("UPDATE booking_requests SET payload=jsonb_set(jsonb_set(jsonb_set(jsonb_set(payload,'{operationalFlightPlan}',$2::jsonb,true),'{management}',$3::jsonb,true),'{importProvenance}',$4::jsonb,true),'{trackCount}',$5::jsonb,true) WHERE id=$1",[candidate.id,JSON.stringify(plan),JSON.stringify(management),JSON.stringify(source),JSON.stringify(trackCount)]);
      }else await client.query('INSERT INTO booking_requests(id,payload,created_at) VALUES($1,$2::jsonb,$3)',[candidate.id,JSON.stringify(saved),candidate.createdAt]);
      const id=randomUUID();
      await client.query('INSERT INTO pilot_flight_imports(id,flight_id,kind,sha256,filename,original,parsed) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)',[id,candidate.id,parsed.kind,parsed.sha256,parsed.filename,req.body,JSON.stringify(parsed)]);
      await client.query('COMMIT');Object.assign(candidate,{operationalFlightPlan:plan,management,importProvenance:source,trackCount});
      res.status(201).json({request:candidate,importId:id});
    }catch(e){if(client)await client.query('ROLLBACK').catch(()=>{});if(!existing){const i=records().indexOf(candidate);if(i>=0)records().splice(i,1);}if(e.code==='23505')throw error(409,'This file was saved concurrently. Reopen review to use the saved flight.');throw e;}
    finally{client?.release();}
  }));
}

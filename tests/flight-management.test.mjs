import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {DOCUMENT_SCHEMA,managementUpdate,mountFlightManagement,validDocument} from '../lib/flight-management.js';

test('classification preserves prior training notes and never infers a category for legacy files',()=>{
  const previous={training:{exercises:'Circuits',progress:'Review'},custom:'retain'};
  const privateFile=managementUpdate({category:'private',exercises:'erase'},previous);
  assert.deepEqual(privateFile,{...previous,category:'private'});assert.equal(previous.category,undefined);
  assert.deepEqual(managementUpdate({category:'training',instructor:'  Test  '},privateFile).training,{...previous.training,instructor:'Test'});
  for(const input of [{category:'other'},{category:'training',progress:42},{category:'training',instructor:'x'.repeat(101)}])assert.throws(()=>managementUpdate(input));
});
test('document payloads reject active HTML, spoofed MIME and oversized files',()=>{
  assert.equal(validDocument(Buffer.from('%PDF-1.7'),'application/pdf'),true);
  for(const [b,t] of [[Buffer.from('<script>'),'application/pdf'],[Buffer.from('%PDF-1.7'),'image/png'],[Buffer.alloc(2097153),'image/jpeg']])assert.equal(validDocument(b,t),false);
});
test('partial forecast decode preserves UTC day rollover, wind, layers and qualified changes',()=>{
  const ctx={};vm.runInNewContext(readFileSync(new URL('../public/pilot-weather-summary.js',import.meta.url),'utf8'),ctx);
  const result=ctx.PilotWx.forecast('TAF TEST 312300Z 0100/0124 27012G19KT SCT010 TEMPO 0103/0106 BKN006');
  for(const part of ['Issued day 31 23:00Z','Valid day 01 00:00Z','gust 19 kt','scattered 1000 ft','Temporary: 0103/0106 UTC, broken 600 ft'])assert.ok(result.includes(part));
  assert.equal(ctx.PilotWx.forecast('NOT AVAILABLE'),'No forecast available.');
  const later=ctx.PilotWx.forecast('TAF TEST 312300Z 0100/0124 PROB30 TEMPO 0104/0108 BKN010',new Date('2026-11-02T01:00:00Z'));
  assert.match(later,/expired/);assert.match(later,/30% probability of temporary conditions: 0104\/0108 UTC, broken 1000 ft/);
});
test('METAR categories handle visibility boundaries, partial reports, RVR and trend groups',()=>{
  const server=readFileSync(new URL('../server.js',import.meta.url),'utf8'),ctx={};
  for(const name of ['currentMetarConditions','parseVisibility','parseMetarVisibilityMiles','parseMetarCeilingFt','getMetarFlightCategory']){
    vm.runInNewContext(server.match(new RegExp('function '+name+'\\([^]*?\\n}'))[0],ctx);
  }
  for(const [report,result] of [['5SM CLR','MVFR'],['P6SM BKN040','VFR'],['1 1/2SM SCT015','IFR'],['M1/4SM CLR','LIFR'],['R24/0600 VV///','UNKNOWN'],['9999 BKN008CB','IFR'],['9999 BKN///','UNKNOWN'],['CLR RMK VIS 9999','UNKNOWN'],['CAVOK TEMPO BKN004','VFR']]){
    assert.equal(ctx.getMetarFlightCategory('METAR TEST 010000Z '+report),result,report);
  }
});

// Optional disposable PostgreSQL integration. Never point this test at a real flight database.
test('SQL-backed pilot routes: authorization, metadata preservation, scoped upload/read/delete and ten-slot limit',{skip:!process.env.PILOT_TEST_PGLITE},async t=>{
  const {PGlite}=await import(process.env.PILOT_TEST_PGLITE);
  const db=new PGlite();t.after(()=>db.close());
  await db.exec('CREATE TABLE booking_requests (id TEXT PRIMARY KEY,payload JSONB);'+DOCUMENT_SCHEMA);
  const records={A:{id:'A',operationalFlightPlan:{actualOff:'09:02Z'}},B:{id:'B'}};
  for(const value of Object.values(records))await db.query('INSERT INTO booking_requests VALUES($1,$2)',[value.id,JSON.stringify(value)]);
  const app=express();app.use(express.json());
  const pool={query:async(...args)=>{const out=await db.query(...args);return {...out,rowCount:out.affectedRows,rows:out.rows.map(r=>r.content?{...r,content:Buffer.from(r.content)}:r)};}};
  mountFlightManagement(app,{requirePilotAccess:(req,res,next)=>req.get('x-fixture')?next():res.sendStatus(401),requireFlightCrew:(req,res)=>req.get('x-fixture')==='crew' || (res.sendStatus(403),false),ready:Promise.resolve(),pool:()=>pool,lookup:id=>records[id],raw:express.raw({type:['application/pdf','image/png','image/jpeg'],limit:'2mb'})});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>new Promise(r=>server.close(r)));
  const url='http://127.0.0.1:'+server.address().port+'/api/booking-ops/requests/';
  const call=(path,opt={})=>fetch(url+path,{...opt,headers:{'x-fixture':'crew',...opt.headers}});
  assert.equal((await fetch(url+'A/documents')).status,401);assert.equal((await call('A/documents',{headers:{'x-fixture':'other'}})).status,403);
  assert.equal((await call('A/management',{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({category:'training',progress:'Practice'})})).status,200);
  const saved=(await db.query('SELECT payload FROM booking_requests WHERE id=\'A\'')).rows[0].payload;
  assert.equal(saved.operationalFlightPlan.actualOff,'09:02Z');assert.equal(saved.management.training.progress,'Practice');
  const bytes=Buffer.from('%PDF-1.7\nfixture'),post=()=>call('A/documents?name=brief.pdf',{method:'POST',headers:{'Content-Type':'application/pdf'},body:bytes});
  const uploaded=await post();assert.equal(uploaded.status,201);const {id}=await uploaded.json();
  const doc=await call('A/documents/'+id);assert.equal(doc.headers.get('cache-control'),'private, no-store');assert.deepEqual(Buffer.from(await doc.arrayBuffer()),bytes);
  assert.equal((await call('B/documents/'+id)).status,404);
  for(let n=1;n<10;n++)assert.equal((await post()).status,201);
  assert.equal((await post()).status,409);assert.equal((await (await call('A/documents')).json()).documents.length,10);
  assert.equal((await call('B/documents/'+id,{method:'DELETE'})).status,404);assert.equal((await call('A/documents/'+id,{method:'DELETE'})).status,204);
  assert.equal((await call('A/documents/'+id)).status,404);assert.equal((await post()).status,201);
});

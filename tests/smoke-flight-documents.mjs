// Explicit localhost-only smoke run against a temporary, DATABASE_URL-free server.
import assert from 'node:assert/strict';
import {PDFDocument} from 'pdf-lib';
const base = process.env.PILOT_QA_URL || 'http://localhost:3133';
assert.ok(['localhost','127.0.0.1'].includes(new URL(base).hostname), 'Local QA only');
const login = await fetch(base+'/api/pilot-login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code:'NGA-LOCAL-QA-ONLY'})});
assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
const send = (url,options={})=>fetch(base+url,{...options,headers:{Cookie:cookie,...options.headers}});
const response = await send('/api/booking-ops/manual-flight',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({dep:'EFHV',arr:'EFHN',flightRules:'VFR',requestDate:'2026-11-18',requestTime:'23:50',estimatedFlightMinutes:40,title:'LOCAL QA document return'})});
const data=await response.json();assert.equal(response.status,201,data.error);
const id=data.request.id;
const movement=(phase,utcDateTime)=>send(`/api/booking-ops/requests/${id}/movements/${phase}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({utcDateTime})});
assert.equal((await movement('out','2026-02-30T12:00')).status,400);
for(const [phase,time] of [['out','2026-11-18T23:50'],['off','2026-11-19T00:00'],['on','2026-11-19T00:40'],['in','2026-11-19T00:45']]) {
  const response=await movement(phase,time),record=await response.json();assert.equal(response.status,200,JSON.stringify(record));
  assert.equal(record.movement.instant,time+':00.000Z');
}
assert.equal((await movement('on','2026-11-18T00:40')).status,400);
for(const [path,count] of [['operational-flight-plan',3],['journey-log',1]]){
  const start=performance.now();
  const response=await send(`/api/booking-ops/requests/${id}/${path}/pdf`,{headers:{Accept:'application/json'}});
  const packet=await response.json();assert.equal(response.status,200,JSON.stringify(packet));
  assert.equal(packet.pages.length,count);assert.ok(packet.filename.startsWith(id));
  const pdf=await PDFDocument.load(Buffer.from(packet.pdf,'base64'));assert.equal(pdf.getPageCount(),count);
  assert.ok(packet.pages.every(page=>page.marks.length>0));
  assert.ok(packet.pages.flatMap(page=>page.marks).filter(mark=>mark.type==='drawText').every(mark=>Number.isFinite(mark.x)&&Number.isFinite(mark.y)));
  if(path==='journey-log') assert.match(packet.pages.flatMap(page=>page.marks).map(mark=>mark.text||'').join('\n'),/BLOCK:    00:55 \(55 MIN\)/);
  console.log(path+': '+count+' matching preview/PDF pages, '+Math.round(performance.now()-start)+' ms');
}
await send('/api/pilot-logout',{method:'POST'});
assert.equal((await fetch(base+`/api/booking-ops/requests/${id}/journey-log/pdf`)).status,401);
console.log('Created temporary QA flight '+id+'; unauthenticated document access rejected.');

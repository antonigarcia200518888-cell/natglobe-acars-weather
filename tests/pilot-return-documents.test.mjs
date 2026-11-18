import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {PDFDocument, StandardFonts, rgb} from 'pdf-lib';
import {capturePdfPage, journeyLogLines} from '../lib/flight-document-preview.js';
import {parseMovementUtc, movementOrderError} from '../lib/flight-movement-time.js';
const read = path => readFileSync(new URL('../'+path,import.meta.url),'utf8');
const tick = () => new Promise(resolve=>setImmediate(resolve));

test('manual movement UTC dates are explicit and reject invalid calendar or clock values',()=>{
  assert.equal(parseMovementUtc('2026-11-19T00:10').toISOString(),'2026-11-19T00:10:00.000Z');
  assert.equal(parseMovementUtc('2028-02-29T23:59').toISOString(),'2028-02-29T23:59:00.000Z');
  for(const input of ['',null,{},'23:50','2026-02-29T10:00','2026-04-31T10:00','2026-11-18T24:00','2026-11-18T12:60','2026-11-18T10:00Z']) assert.equal(parseMovementUtc(input),null);
  const source=read('views/operational-flight-plan.html');
  assert.equal([...source.matchAll(/type="datetime-local" step="60" data-manual-utc=/g)].length,4);
  assert.match(source,/JSON.stringify\(\{ utcDateTime \}\)/);
});

test('movement chronology crosses midnight only with a dated timestamp and permits equal-minute events',()=>{
  const plan={actualOutIso:'2026-11-18T23:50:00Z',actualOffIso:'2026-11-19T00:00:00Z',actualInIso:'2026-11-19T00:45:00Z'};
  assert.equal(movementOrderError(plan,'actualOn','2026-11-19T00:40:00Z'),'');
  assert.equal(movementOrderError(plan,'actualOn','2026-11-19T00:45:00Z'),'');
  assert.match(movementOrderError(plan,'actualOn','2026-11-18T00:40:00Z'),/at or after OUT/);
  assert.match(movementOrderError(plan,'actualOn','2026-11-19T00:46:00Z'),/at or before IN/);
  assert.equal(movementOrderError({actualOut:'2350Z/0150L'},'actualOn','2026-11-19T00:40:00Z'),'');
});

test('journey log uses dated saved actuals, including midnight, without inventing legacy dates',()=>{
  const plan={aircraftRegistration:'OH-PMK',departure:'EFHV',destination:'EFHN',actualOutIso:'2026-11-18T23:50:00Z',actualOffIso:'2026-11-19T00:00:00Z',actualOnIso:'2026-11-19T00:40:00Z',actualInIso:'2026-11-19T00:45:00Z',briefingRemarks:'Long operational note '.repeat(50)};
  const lines=journeyLogLines({id:'TEST'},plan,'2026-11-19T01:00:00Z');
  assert.match(lines.join('\n'),/AIRBORNE: 00:40 \(40 MIN\)/);
  assert.match(lines.join('\n'),/BLOCK:    00:55 \(55 MIN\)/);
  assert.ok(lines.every(line=>line.length<=78));
  const legacy=journeyLogLines({id:'TEST'},{actualOut:'2350Z',actualIn:'0010Z'});
  assert.match(legacy.join('\n'),/Legacy clock-only/);
  assert.match(legacy.join('\n'),/BLOCK:    NOT AVAILABLE/);
  assert.doesNotMatch(legacy.join('\n'),/00:20/);
});

test('SVG preview captures the same positions, fonts, lines and rectangles as the PDF',async()=>{
  const pdf=await PDFDocument.create(), courier=await pdf.embedFont(StandardFonts.Courier), courierBold=await pdf.embedFont(StandardFonts.CourierBold);
  const page=pdf.addPage([595,842]);
  const packet=capturePdfPage(page,{width:595,height:842,courier,courierBold});
  page.drawText('EFHV -> EFHN',{x:40,y:750,size:12,font:courier,color:rgb(0,0,0)});
  page.drawText('JOURNEY LOG',{x:40,y:780,size:12,font:courierBold});
  page.drawLine({start:{x:40,y:740},end:{x:550,y:740},thickness:1});
  page.drawRectangle({x:40,y:100,width:25,height:25,borderWidth:1,borderColor:rgb(0,1,0)});
  assert.equal(packet.marks.length,4);assert.equal(packet.marks[0].y,750);
  assert.equal(packet.marks[0].mono,true);assert.equal(packet.marks[1].bold,true);
  assert.equal(packet.marks[3].fill,false);
  assert.equal((await PDFDocument.load(await pdf.save())).getPageCount(),1);
});

function lockFixture(fetcher) {
  const nodes=new Map(),events=new Map(),timers=new Map(),classes=new Set(),calls=[];let timerId=0,modal;
  const make=()=>({dataset:{},children:[],listeners:{},value:'code',disabled:false,hidden:false,
    setAttribute(){},focus(){this.focused=true;},querySelector:by,append(...kids){this.children.push(...kids);},
    addEventListener(key,fn){this.listeners[key]=fn;},reset(){},showModal(){this.open=true;},close(){this.open=false;this.listeners.close?.();}});
  const by=key=>{if(!nodes.has(key))nodes.set(key,make());return nodes.get(key);};
  const win={addEventListener:(k,fn)=>events.set('window:'+k,fn),dispatchEvent(){},PublicKeyCredential:{},SimpleWebAuthnBrowser:{startAuthentication:async()=>({id:'synthetic'})}};win.top=win;
  const doc={hidden:false,readyState:'complete',activeElement:make(),documentElement:{classList:{add:k=>classes.add(k),remove:k=>classes.delete(k)}},body:make(),
    createElement:()=>modal=make(),addEventListener:(k,fn)=>events.set(k,fn)};
  const ctx={window:win,document:doc,sessionStorage:{getItem:()=>null,removeItem(){}},Date,Promise,CustomEvent:class {},
    AbortSignal,fetch:async(url,options)=>{calls.push(url);return fetcher?fetcher(url,options):{ok:true,json:async()=>url==='/api/pilot-session'?{session:{role:'COMMANDER',name:'QA'}}:{role:'COMMANDER',name:'QA'}};},
    setTimeout:(fn,delay)=>{timers.set(++timerId,{fn,delay});return timerId;},clearTimeout:id=>timers.delete(id)};
  vm.runInNewContext(read('public/pilot-session-lock.js'),ctx);
  return {win,doc,by,events,timers,classes,calls,modal};
}

test('foreground return locks without navigation, keeps the modal above the app and asks for a fresh login',async()=>{
  const f=lockFixture();await tick();
  assert.equal(f.win.NGAPilotLock.locked,true);assert.equal(f.modal.open,true);
  assert.equal([...f.timers.values()][0].delay,5000);
  let escaped=false;f.modal.listeners.cancel({preventDefault(){escaped=true;}});assert.equal(escaped,true);
  await f.by('form').onsubmit({preventDefault(){}});await tick();
  assert.equal(f.win.NGAPilotLock.locked,false);assert.equal(f.modal.open,false);
  f.doc.hidden=true;f.events.get('visibilitychange')();
  assert.equal(f.win.NGAPilotLock.locked,true);assert.equal(f.modal.dataset.phase,'splash');
  f.doc.hidden=false;f.events.get('visibilitychange')();await tick();
  assert.equal(f.calls.filter(url=>url==='/api/pilot-logout').length,2);
  assert.equal(f.classes.has('nga-session-pending'),true);
  [...f.timers.values()].at(-1).fn();assert.equal(f.modal.dataset.phase,'unlock');
  assert.equal(f.events.has('hashchange'),false);
});

test('failed verification and another pilot profile never expose the preserved workspace',async()=>{
  let ok=false;
  const f=lockFixture(async url=>({ok:url==='/api/pilot-login'?ok:true,json:async()=>url==='/api/pilot-session'?{session:{role:'COMMANDER',name:'QA'}}:{role:'DISPATCH',name:'OTHER',error:'Denied'}}));
  await tick();await f.by('form').onsubmit({preventDefault(){}});await tick();
  assert.equal(f.win.NGAPilotLock.locked,true);
  ok=true;await f.by('form').onsubmit({preventDefault(){}});await tick();
  assert.equal(f.win.NGAPilotLock.locked,true);
  assert.match(f.by('[data-status]').textContent,/same pilot/);
});

test('rapid background/foreground events deduplicate session clearing before unlock',async()=>{
  let finish;
  const f=lockFixture(async url=>url==='/api/pilot-logout'?new Promise(resolve=>{finish=()=>resolve({ok:true,json:async()=>({ok:true})});}):{ok:true,json:async()=>({session:{role:'COMMANDER',name:'QA'}})});
  await tick();f.doc.hidden=true;f.events.get('visibilitychange')();f.doc.hidden=false;f.events.get('visibilitychange')();
  assert.equal(f.calls.filter(url=>url==='/api/pilot-logout').length,1);
  assert.equal(f.by('[data-unlock]').disabled,true);finish();await tick();assert.equal(f.by('[data-unlock]').disabled,false);
});

test('offline return stays locked with explicit connection error, never a fake biometric success',async()=>{
  const f=lockFixture(async()=>{throw Error('offline');});await tick();
  assert.equal(f.win.NGAPilotLock.locked,true);assert.equal(f.by('[data-unlock]').disabled,true);
  assert.match(f.by('[data-status]').textContent,/Connection required/);
  assert.equal(f.by('[data-retry]').hidden,false);
});

test('document viewer exposes stable return paths, Escape dialog semantics and no top-page PDF navigation',()=>{
  const source=read('public/pilot-flight-documents.js'), css=read('public/pilot-flight-documents.css');
  new vm.Script(source);new vm.Script(read('public/pilot-session-lock.js'));
  assert.match(source,/Back to flight plan/);assert.match(source,/Back to EFB/);assert.match(source,/dialog.showModal\(\)/);
  assert.doesNotMatch(source,/location\.(assign|replace)|window.open|requestFullscreen/);
  assert.match(source,/host.append\(pane\)/);assert.match(source,/opener\?\.focus/);
  assert.match(source,/Courier New/);assert.match(css,/@media print/);
  assert.match(css,/stroke:none !important/);
  assert.doesNotMatch(read('views/booking-ops.html'),/data-open-hangar|pilot-hangar/);
  assert.doesNotMatch(read('package.json'),/"three"/);
});

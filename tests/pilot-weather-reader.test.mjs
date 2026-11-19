import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {PDFDocument} from 'pdf-lib';
import {dispatchToPdfBuffer} from '../lib/dispatch-document.js';
const read = path=>readFileSync(new URL('../'+path,import.meta.url),'utf8');
const shell=read('views/booking-ops.html');

test('long dispatch reports preview every PDF page, with all lines inside the sheet',async()=>{
  const make=(fixture,preview)=>dispatchToPdfBuffer(fixture,preview,{getMetarFlightCategory:()=>'VFR',generateAutoRemark:()=>'FIXTURE'});
  const pages=[], fixture={date:'2026-01-01',timeUtc:'12:00 UTC',dep:'EFHK',arr:'EFTP',includeDepMetar:true,depWx:{airport:'EFHK',metar:'TEST FORECAST '.repeat(500)}};
  const bytes=await make(fixture,pages), pdf=await PDFDocument.load(bytes);
  assert.equal(pdf.getPageCount(),pages.length); assert.ok(pages.length>1);
  assert.equal(pages.at(-1).at(-1).text,'END OF REPORT');
  assert.ok(pages.flat().every(line=>line.y>0 && line.y<=841.89-36)); assert.equal(pages[0][0].bold,true);
  const legacy=await make({...fixture,depWx:null}); assert.equal((await PDFDocument.load(legacy)).getPageCount(),1);
});

function readerFixture(fetch){
  const nodes=new Map(), revoked=[], routes=[], timeouts=[];
  const make=()=>({children:[],listeners:{},attrs:{},hidden:false,focus(){this.focused=true;},
    append(...kids){this.children.push(...kids)},replaceChildren(...kids){this.children=kids;},
    setAttribute(k,v){this.attrs[k]=v},removeAttribute(k){delete this.attrs[k];},
    addEventListener(k,fn){this.listeners[k]=fn},click(){return this.listeners.click?.()}});
  const by=id=>{if(!nodes.has(id))nodes.set(id,make());return nodes.get(id)};
  const modal=Object.assign(make(),{querySelector:by,showModal(){this.open=true},close(){this.open=false;this.listeners.close()}});
  const context={window:{},document:{createElement:make,createElementNS:make,createDocumentFragment:make},navigator:{},URLSearchParams,
    URL:{createObjectURL:()=>'blob:local',revokeObjectURL:s=>revoked.push(s)},AbortController,File,atob,
    setTimeout:fn=>{timeouts.push(fn);return fn;},clearTimeout:()=>{},
    fetch:(url,options)=>{routes.push({url,options});return fetch(url,options)}};
  vm.runInNewContext(read('public/pilot-weather-viewer.js'),context);
  return {reader:context.window.createWeatherReader(modal),modal,by,revoked,routes,timeouts,opener:make()};
}
const ready=()=>({ok:true,json:async()=>({pageWidth:595.28,pageHeight:841.89,pages:[[{text:'<METAR>',size:10,x:20,y:30,ink:[0,0,0]}]],pdf:Buffer.from('%PDF-1.7').toString('base64')})});
const tick=()=>new Promise(resolve=>setImmediate(resolve));
test('reader keeps the route options, shows the same packet in both modes, and restores focus',async()=>{
  const {reader,modal,by,revoked,routes,opener}=readerFixture(ready);
  reader.open('dep=EFHK&arr=EFTP&includeArrTaf=false',opener);await tick();
  assert.equal(modal.open,true);assert.match(routes[0].url,/includeArrTaf=false&preview=1/);assert.equal(routes[0].options.cache,'no-store');
  assert.equal(by('#wxReaderDownload').download,'NatGlobe-WX-EFHK-EFTP.pdf');assert.equal(by('#wxReaderStatus').textContent,'1 page · Generated briefing');
  by('#wxReaderMode').click();assert.equal(by('#wxReaderMode').attrs['aria-pressed'],'true');
  assert.equal(by('#wxReaderPages').children[0].children[0].children[0].textContent,'<METAR>');assert.equal(routes.length,1);
  by('#wxReaderBack').click();assert.equal(modal.open,false);assert.equal(opener.focused,true);assert.deepEqual(revoked,['blob:local']);
});
test('Back works while loading; stale responses cannot overwrite a reopened report; errors can retry',async()=>{
  const requests=[];const {reader,modal,by,routes,opener}=readerFixture(()=>new Promise(resolve=>requests.push(resolve)));
  reader.open('dep=EFHK&arr=EFTP',opener);by('#wxReaderBack').click();assert.equal(routes[0].options.signal.aborted,true);
  reader.open('dep=EFTP&arr=EFHK',opener);requests[0](ready());requests[1]({ok:false});await tick();
  assert.equal(modal.open,true);assert.equal(by('#wxReaderRetry').hidden,false);assert.equal(by('#wxReaderDownload').hidden,true);
  const retry=by('#wxReaderRetry').click();requests[2](ready());await retry;
  assert.equal(by('#wxReaderDownload').download,'NatGlobe-WX-EFTP-EFHK.pdf');assert.equal(by('#wxReaderRetry').hidden,true);
});
test('weather entry has no mock airport codes; install artwork is local, square and correctly sized',()=>{
  for(const id of ['weatherDep','weatherArr','weatherAltn']){
    const input=shell.match(new RegExp('<input id="'+id+'"[^>]*>'))[0];
    assert.match(input,/maxlength="4"/);assert.match(input,/pattern="\[A-Z\]\{4\}"/);assert.doesNotMatch(input,/list=|placeholder="[A-Z]/);
  }
  const manifest=JSON.parse(read('public/pilot-manifest.webmanifest'));
  for(const {src,sizes} of [...manifest.icons,{src:'/pilot-natglobe-180.png',sizes:'180x180'}]){
    const png=readFileSync(new URL('../public'+src,import.meta.url));
    assert.equal(png.readUInt32BE(16)+'x'+png.readUInt32BE(20),sizes); assert.ok(read('public/pilot-sw.js').includes(src));
  }
});

test('weather PDF timeout exposes retry and leaves Back available',async()=>{
  const {reader,modal,by,timeouts,opener}=readerFixture((url,{signal})=>new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(new Error('aborted')))));
  reader.open('dep=EFHK&arr=EFTP',opener);timeouts[0]();await tick();
  assert.match(by('#wxReaderStatus').textContent,/timed out/);assert.equal(by('#wxReaderRetry').hidden,false);
  by('#wxReaderBack').click();assert.equal(modal.open,false);assert.equal(opener.focused,true);
});

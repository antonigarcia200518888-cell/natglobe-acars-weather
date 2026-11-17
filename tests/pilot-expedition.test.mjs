import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const source=name=>readFileSync(new URL('../public/'+name,import.meta.url),'utf8');

test('fresh unlock splash lasts three seconds, can be skipped, and is not replayed in the session',()=>{
  const stored=new Map(),classes=new Set(),events=new Map();let timer;
  const ctx={sessionStorage:{getItem:k=>stored.get(k),setItem:(k,v)=>stored.set(k,v)},performance:{now:()=>10},
    document:{documentElement:{classList:{add:k=>classes.add(k),remove:k=>classes.delete(k)}},addEventListener:(k,fn)=>events.set(k,fn),removeEventListener:k=>events.delete(k)},
    window:{addEventListener(){}},setTimeout:(fn,delay)=>{timer={fn,delay};return 1;},clearTimeout(){}};
  vm.runInNewContext(source('pilot-startup.js'),ctx);
  assert.equal(timer.delay,3000);assert.equal(classes.has('nga-startup'),true);
  events.get('keydown')();assert.equal(classes.has('nga-startup'),false);
  timer=null;vm.runInNewContext(source('pilot-startup.js'),ctx);assert.equal(timer,null);
});

test('shared flight facade uses the OFP view without mutating passenger booking data',()=>{
  const ctx={window:{},console};vm.runInNewContext(source('pilot-flight-context.js'),ctx);
  let file={id:'A',dep:'EFHV',arr:'EFHN',operationalFlightPlan:{departure:'EFHK',destination:'EETN',route:'EFHK DCT EETN'}};
  const model=ctx.window.createPilotFlightContext({getFlight:()=>file,getFlights:()=>[file]});
  assert.equal(model.current().dep,'EFHK');assert.equal(file.dep,'EFHV');
  let observed;model.subscribe(v=>observed=v);file=null;model.notify();assert.equal(observed,null);
});

test('replay UI remains manual, handles map startup, stops at gaps and clears on flight switch',()=>{
  const nodes=new Map(),frames=new Map(),events=new Map();let frameId=0,subscription;
  const node=()=>({hidden:false,disabled:false,value:0,innerHTML:'',textContent:'',setAttribute(){},append(){},querySelector:s=>{
    const key=s.match(/"([^"]+)"/)[1];if(!nodes.has(key))nodes.set(key,node());return nodes.get(key);
  }});
  const map={ready:false,setMode(){},setTrack(g){this.geometry=g;},fitTrack(){this.fitted=true;},setRotating(v){this.rotating=v;},setReplayPosition(p){this.position=p;},clearReplay(){this.cleared=true;}};
  const ctx={window:{addEventListener:(k,fn)=>events.set(k,fn)},document:{hidden:false,createElement:node,addEventListener:(k,fn)=>events.set(k,fn)},
    matchMedia:()=>({matches:false}),performance:{now:()=>1000},requestAnimationFrame:fn=>{frames.set(++frameId,fn);return frameId;},cancelAnimationFrame:id=>frames.delete(id)};
  vm.runInNewContext(source('pilot-replay.js'),ctx);
  const replay=ctx.window.createPilotReplay({map,container:node(),isVisible:()=>true,context:{current:()=>({id:'A'}),subscribe:fn=>subscription=fn}});
  const track={summary:{pointCount:3,replayAvailable:true,distanceNm:4},altitudeReference:'Synthetic test feet',points:[
    {lat:60,lon:24,time:'2026-11-17T12:00:00Z',altitudeFt:1000,speedKt:100,segment:0},
    {lat:60.1,lon:24.1,time:'2026-11-17T12:01:00Z',altitudeFt:1200,speedKt:105,segment:0},
    {lat:60.5,lon:24.5,time:'2026-11-17T12:05:00Z',altitudeFt:1400,speedKt:110,segment:0}]};
  replay.load(track);assert.equal(nodes.get('play').textContent,'Play');assert.equal(nodes.get('draft').hidden,true);
  map.ready=true;replay.onReady();assert.equal(map.rotating,false);assert.equal(map.fitted,true);
  assert.equal((nodes.get('profiles').innerHTML.match(/M/g)||[]).length,4); // two separate parts per chart
  nodes.get('time').value=500;nodes.get('time').oninput();assert.equal(nodes.get('gap').hidden,false);
  assert.equal(map.position.lon,24.1);nodes.get('gap').onclick();assert.equal(map.position.lon,24.5);
  replay.load({...track,summary:{...track.summary,replayAvailable:false}});assert.equal(nodes.get('play').disabled,true);assert.equal(nodes.get('time').disabled,true);
  subscription({id:'B'});assert.equal(map.cleared,true);
});

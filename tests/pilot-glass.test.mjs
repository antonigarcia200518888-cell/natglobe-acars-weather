import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const src=fs.readFileSync(new URL('../public/pilot-glass.js',import.meta.url),'utf8');
function host({solid=false, reduceMotion=false, reducedTransparency=false, available=true}={}) {
  const target=()=>({dataset:{},events:{},addEventListener(type,fn){this.events[type]=fn;}});
  const canvas=target(), chrome=target(), root=target(), doc=target(), win=target();
  root.dataset={efbMaterial:solid?'solid':'glass',efbTheme:'night'};
  Object.assign(doc,{documentElement:root,body:target(),getElementById:()=>canvas,querySelectorAll:s=>s.includes('surface')?[chrome]:[]});
  let allocations=0,loads=0,id=0,now=0,paints=[];
  const frames=new Map(), observers=new Map();
  win.PilotGlassRenderer=()=>{allocations++;return available && {frameInterval:33,paint:(night,p)=>{paints.push([night,p.amount]);return true;}};};
  vm.runInNewContext(src,{document:doc,window:win,console,Image:class{set src(v){assert.equal(v,'/pilot-glass-relief.svg');loads++;this.onload();}},
    matchMedia:q=>Object.assign(target(),{matches:q.includes('motion')?reduceMotion:q.includes('transparency')&&reducedTransparency}),
    requestAnimationFrame:fn=>{frames.set(++id,fn);return id;},cancelAnimationFrame:key=>frames.delete(key),
    MutationObserver:class{constructor(fn){this.fn=fn;}observe(el){observers.set(el,this.fn);}}});
  const tick=()=>{const batch=[...frames.values()];frames.clear();now+=34;batch.forEach(fn=>fn(now));};
  return {root,canvas,chrome,doc,win,frames,paints,tick,sync:()=>observers.get(root)(),counts:()=>[loads,allocations]};
}

test('Solid and reduced transparency allocate no texture or WebGL context',()=>{
  for(const opts of [{solid:true},{reducedTransparency:true}]) {
    const h=host(opts);assert.deepEqual(h.counts(),[0,0]);assert.equal(h.frames.size,0);
    assert.equal(h.canvas.dataset.state,'preference');
  }
  const h=host({available:false});assert.equal(h.root.dataset.pilotGlass,undefined);assert.equal(h.frames.size,0);
});
test('chrome sleeps at idle, repaints on theme changes and restores without reloading flight state',()=>{
  const h=host();h.tick();assert.equal(h.root.dataset.pilotGlass,'ready');assert.equal(h.frames.size,0);
  assert.equal(h.win.events.pointermove,undefined);
  h.root.dataset.efbTheme='day';h.sync();h.tick();assert.deepEqual(h.paints.at(-1),[false,0]);
  h.canvas.events.webglcontextlost({preventDefault(){}});assert.equal(h.root.dataset.pilotGlass,undefined);
  h.canvas.events.webglcontextrestored();h.tick();assert.equal(h.root.dataset.pilotGlass,'ready');assert.deepEqual(h.counts(),[1,2]);
  h.root.dataset.efbMaterial='solid';h.sync();assert.equal(h.root.dataset.pilotGlass,undefined);
});
test('reduced motion still shows static glass, and does not schedule touch animation',()=>{
  const h=host({reduceMotion:true});h.tick();
  h.chrome.events.pointerdown({clientX:10,clientY:20,target:{closest:()=>true}});
  assert.equal(h.root.dataset.pilotGlass,'ready');assert.equal(h.frames.size,0);
});

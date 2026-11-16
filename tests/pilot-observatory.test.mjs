import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const css = fs.readFileSync(new URL('../public/pilot-observatory.css', import.meta.url), 'utf8');
const rgb = hex => hex.match(/[a-f\d]{2}/gi).map(n => parseInt(n,16));
const luminance = color => color.map(n => { const v=n/255; return v<=.04045?v/12.92:((v+.055)/1.055)**2.4; }).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0);
const contrast = (a,b) => { const x=luminance(a),y=luminance(b); return (Math.max(x,y)+.05)/(Math.min(x,y)+.05); };

test('observatory text remains readable over the brightest and darkest map pixels',()=>{
  const themes = [...css.matchAll(/--deck-surface:rgba\(([^)]+)\);([^}]+)/g)];
  assert.equal(themes.length,2);
  for (const [,rgba,tokens] of themes) {
    const [r,g,b,alpha] = rgba.split(',').map(Number);
    for(const background of [0,255]) {
      const surface=[r,g,b].map(n=>n*alpha+background*(1-alpha));
      for(const token of ['ink','muted','caution','warning']) {
        const [,hex] = tokens.match(new RegExp(`--deck-${token}:(#[a-f0-9]{6});`));
        assert.ok(contrast(rgb(hex),surface)>=4.5,`${token} must meet text contrast on ${background}`);
      }
    }
  }
});

test('tablet rail reserves its actual width; portrait folder and map have separate rows',()=>{
  assert.match(css,/@media \(orientation:landscape\) and \(min-width:960px\) and \(min-height:500px\)/);
  assert.match(css,/padding-left:calc\(var\(--rail-width\) \+ var\(--rail-inset\) \+ 8px\)/);
  assert.match(css,/grid-template-areas:"map switch" "map inspector"/);
  assert.match(css,/grid-template-areas:"map" "file" "pulse"/);
  assert.match(css,/grid-template-areas:"map map" "file pulse"/);
  assert.match(css,/#opsGlobeCanvas:not\(\[hidden\]\) \+ #opsVectorMap \{ visibility:hidden/);
  assert.match(css,/\.cockpit-empty-flight\[hidden\] \{ display:none; \}/);
  assert.match(css,/prefers-reduced-transparency:reduce/);
  assert.match(css,/--deck-surface:var\(--deck-solid\); --deck-blur:none/);
});

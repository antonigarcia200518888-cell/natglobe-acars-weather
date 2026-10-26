import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const css = fs.readFileSync(new URL('../public/pilot-observatory.css', import.meta.url), 'utf8');
const rgb = hex => hex.match(/[a-f\d]{2}/gi).map(n => parseInt(n,16));
const luminance = color => color.map(n => { const v=n/255; return v<=.04045?v/12.92:((v+.055)/1.055)**2.4; }).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0);
const contrast = (a,b) => { const x=luminance(a),y=luminance(b); return (Math.max(x,y)+.05)/(Math.min(x,y)+.05); };

test('observatory text remains readable over the brightest and darkest map pixels',()=>{
  for (const theme of [
    {tint:[247,248,247],alpha:.91,ink:'#19231f',muted:'#52605a',caution:'#80520a',warning:'#ad283b'},
    {tint:[31,35,34],alpha:.84,ink:'#f3f6f3',muted:'#bac5be',caution:'#e9b86f',warning:'#ffaaaa'}
  ]) {
    assert.ok(css.includes(`rgba(${theme.tint.join(',')},${String(theme.alpha).slice(1)})`));
    for(const background of [0,255]) {
      const surface=theme.tint.map(n=>n*theme.alpha+background*(1-theme.alpha));
      for(const token of ['ink','muted','caution','warning']) {
        assert.ok(css.includes(theme[token]));
        assert.ok(contrast(rgb(theme[token]),surface)>=4.5,`${token} must meet text contrast on ${background}`);
      }
    }
  }
});

test('tablet rail reserves space and portrait cards return to document flow',()=>{
  assert.match(css,/@media\(min-width:760px\) and \(min-height:600px\)/);
  assert.match(css,/padding-right:88px/);
  assert.match(css,/@media\(max-width:999px\), \(max-height:730px\)/);
  assert.match(css,/position:relative; inset:auto; grid-column:1\/-1/);
  assert.match(css,/\.cockpit-empty-flight\[hidden\] \{ display:none; \}/);
  assert.match(css,/prefers-reduced-transparency:reduce/);
  assert.match(css,/--deck-surface:var\(--deck-solid\); --deck-blur:none/);
});

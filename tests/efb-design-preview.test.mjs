import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const read = name => fs.readFileSync(new URL(`../public/efb-preview/${name}`, import.meta.url), 'utf8');
const html = read('index.html');
const css = read('efb-dashboard.css');
const js = read('efb-dashboard.js');
const context = vm.createContext({window:{}});
vm.runInContext(js.slice(js.indexOf('  const airports'),js.indexOf('  const element')),context);
const model = context.window.efbPreviewModel;
const sample = {departure:'EFHV',destination:'EFTP',speed:110,altitude:4500,rules:'VFR'};

test('preview JavaScript and the first-paint theme script parse', () => {
  new vm.Script(js);
  for (const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
});

test('sample planning is deterministic, symmetric and distinguishes units', () => {
  const plan = model.calculatePlan(sample);
  assert.ok(plan.distance > 58 && plan.distance < 60);
  assert.ok(Math.abs(plan.minutes - plan.distance / 110 * 60) < 1e-10);
  const reverse = model.calculatePlan({...sample,departure:'EFTP',destination:'EFHV'});
  assert.ok(Math.abs(reverse.distance - plan.distance) < 1e-10);
  assert.equal(model.calculatePlan({...sample,speed:220}).minutes, plan.minutes / 2);
});

test('invalid sample plans are rejected before changing the map model', () => {
  for (const changes of [{departure:'ZZZZ'},{departure:'toString'},{destination:'EFHV'},{speed:0},{speed:NaN},{speed:601},{altitude:-1},{altitude:Infinity},{rules:'UNKNOWN'}]) {
    assert.throws(()=>model.calculatePlan({...sample,...changes}));
  }
});

test('five named tabs point to unique panels with initial roving focus', () => {
  const ids = [...html.matchAll(/\sid="([^"]+)"/g)].map(m=>m[1]);
  assert.equal(ids.length,new Set(ids).size);
  for (const view of ['maps','plates','documents','flight-plan','settings']) {
    assert.ok(ids.includes(`panel-${view}`));
    assert.ok(ids.includes(`tab-${view}`));
    assert.match(html,new RegExp(`aria-controls="panel-${view}"`));
    assert.match(html,new RegExp(`aria-labelledby="tab-${view}"`));
  }
  assert.equal([...html.matchAll(/role="tab"/g)].length,5);
  assert.match(js,/\['ArrowLeft','ArrowRight','Home','End'\]/);
  assert.match(js,/window\.addEventListener\('hashchange'/);
  assert.match(js,/element\('main-content'\)\.focus\(\{preventScroll:true\}\)/);
});

test('preview keeps operational controls, location and caches outside its scope', () => {
  assert.doesNotMatch(js,/fetch\(|\/api\/|getCurrentPosition|watchPosition|serviceWorker\.register/);
  assert.match(html,/SAMPLE DATA · NOT FOR NAVIGATION/);
  assert.match(html,/No approved chart provider is connected/);
  assert.match(html,/GPS accuracy[\s\S]*?Not connected/);
  assert.match(html,/id="batteryLevel">N\/A/);
  assert.match(js,/typeof navigator\.getBattery === 'function'/);
  assert.match(js,/preferenceKey = 'nga-efb-design-preview-v1'/);
  assert.match(js,/if \(!document.hidden\) clockTimer = setInterval/);
  assert.match(js,/if \(activeView !== 'maps' \|\| mapPanel !== 'map'\) \{ routeNeedsFit = true; return; \}/);
});

test('theme tokens meet requested palette, touch size and reduced-motion behavior', () => {
  for (const color of ['#12161A','#F5F7FA','#FFFFFF','#1A1F26','#2B6CB0','#2D3748']) assert.ok(css.includes(color));
  assert.match(css,/--tap: 48px/);
  assert.match(css,/min-height: var\(--tap\)/);
  assert.match(css,/height: 100dvh/);
  assert.match(css,/safe-area-inset-bottom/);
  assert.match(css,/prefers-reduced-motion: reduce/);
  assert.match(css,/@supports \(\(backdrop-filter/);
  assert.match(css,/:root:not\(\[data-material="solid"\]\) \.glass-surface/);
  assert.match(css,/prefers-reduced-transparency: reduce/);
  assert.match(css,/prefers-contrast: more/);
  assert.match(css,/\.card \{ background: var\(--surface\)/);
  assert.match(js,/JSON\.stringify\(\{theme,layout,material\}\)/);
  assert.match(html,/data-material-choice="solid"/);
  assert.match(html,/data-material-choice="glass"/);
});

test('floating panels leave room for route endpoints on tablet and phone', () => {
  const tablet = model.routePadding({width:1194,height:704,layout:'split',sheetWidth:310,sheetHeight:480});
  assert.ok(tablet.paddingTopLeft[0] > 310 + 18);
  assert.ok(tablet.paddingTopLeft[1] > 80);
  assert.ok(tablet.paddingBottomRight[1] >= 120);
  const phone = model.routePadding({width:390,height:680,layout:'split',sheetWidth:370,sheetHeight:164});
  assert.ok(phone.paddingTopLeft[0] < 60);
  assert.ok(phone.paddingBottomRight[1] >= 164 + 120);
  const expanded = model.routePadding({width:1194,height:704,layout:'map',sheetWidth:0,sheetHeight:0});
  assert.ok(expanded.paddingTopLeft[0] < tablet.paddingTopLeft[0]);
  const short = model.routePadding({width:390,height:340,layout:'split',sheetHeight:164});
  assert.ok(340 - short.paddingTopLeft[1] - short.paddingBottomRight[1] >= 100);
});

test('display restoration handles missing, malformed and saved settings', () => {
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  for (const [stored,expected] of [[null,{}],['broken',{}],['null',{}],['{"theme":"day","material":"solid"}',{theme:'day',material:'solid'}]]) {
    const dataset = {};
    vm.runInNewContext(script,{localStorage:{getItem:()=>stored},document:{documentElement:{dataset}}});
    assert.deepEqual(dataset,expected);
  }
});

function contrast(a,b) {
  const luminance = hex => {
    const rgb = hex.match(/[\da-f]{2}/gi).map(part=>parseInt(part,16)/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4);
    return rgb[0]*.2126+rgb[1]*.7152+rgb[2]*.0722;
  };
  const [light,dark] = [luminance(a),luminance(b)].sort((x,y)=>y-x);
  return (light+.05)/(dark+.05);
}

test('day/night text, action and status color pairs have at least 4.5:1 contrast', () => {
  const blocks = [...css.matchAll(/:root(?:\[data-theme="day"\])?\s*\{([^}]+)\}/g)].slice(0,2);
  for (const block of blocks) {
    const tokens = Object.fromEntries([...block[1].matchAll(/--([\w-]+):\s*(#[\da-f]{6});/gi)].map(m=>[m[1],m[2]]));
    for (const fg of ['text','muted','teal']) for (const bg of ['bg','surface','surface-raised']) assert.ok(contrast(tokens[fg],tokens[bg])>=4.5, `${fg}/${bg}`);
    assert.ok(contrast(tokens['accent-text']||'#FFFFFF',tokens.accent)>=4.5);
    for (const status of ['warning','caution','normal']) assert.ok(contrast(tokens[status],tokens[`${status}-bg`])>=4.5,status);
  }
});

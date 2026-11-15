import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const read = path => fs.readFileSync(new URL('../' + path, import.meta.url), 'utf8');
const html = read('views/booking-ops.html');
function run(name, mocks, ...args) {
  const body = html.match(new RegExp('    function ' + name + '\\([^]*?\\n    }'));
  assert.ok(body, name);
  return vm.runInNewContext('(' + body[0] + ')(...args)', {...mocks,args});
}
test('compact Flights only opens a file for an explicit selection; returning keeps it loaded', () => {
  const bookingsWorkspace = {dataset:{}}, selected = {id:'TEST'}, targets=[];
  const mocks = {bookingsWorkspace,selected,activeWorkspace:'flights',el:id=>({focus(){targets.push(id);}})};
  run('setFlightPane',{...mocks,selected:null},'file',true);
  assert.equal(bookingsWorkspace.dataset.pane,'list');
  run('setFlightPane',mocks,'file',true);
  assert.equal(bookingsWorkspace.dataset.pane,'file');
  run('setFlightPane',mocks,'list',true);
  assert.deepEqual(targets,['filterQuery','flightFileHeading','filterQuery']);
  assert.equal(selected.id,'TEST');
});
test('manifest shortcut selects the visible passenger view; pilot-only and empty records cannot navigate there', () => {
  const actions=[], mocks = {switchOpsWorkspace:x=>actions.push(x),setFlightDetailTab:x=>actions.push(x),manifestList:{scrollIntoView(){actions.push('scroll');}}};
  for(const selected of [null,{manualFlight:true}]) run('openPassengerManifest',{...mocks,selected});
  assert.deepEqual(actions,[]);
  run('openPassengerManifest',{...mocks,selected:{manualFlight:false}});
  assert.deepEqual(actions,['flights','passengers','scroll']);
});
test('radar opens on demand and stops playback when the briefing is selected', () => {
  let loads=0, stops=0;
  const nodes={}, tabs=['briefing','radar'].map(view=>({dataset:{weatherView:view},setAttribute(k,v){this[k]=v;}}));
  const mocks={weatherOpsPane:{dataset:{}},document:{querySelectorAll:()=>tabs},el:id=>nodes[id]??={},activeWorkspace:'weather',window:{requestAnimationFrame:fn=>fn()},weatherRadarMap:null,loadWeatherRadar:async()=>{loads++;},setWeatherRadarPlayback:()=>{stops++;}};
  run('setWeatherView',mocks,'radar');
  assert.equal(loads,1); assert.equal(nodes.weatherRadarPanel.hidden,false); assert.equal(tabs[1].tabIndex,0);
  run('setWeatherView',mocks,'briefing');
  assert.equal(stops,1); assert.equal(nodes.weatherBriefingPanel.hidden,false); assert.equal(nodes.weatherRadarPanel.hidden,true); assert.equal(tabs[0]['aria-selected'],'true');
});
test('sidebar preference updates the accessible name and expanded state on both toggles', () => {
  const button=()=>({setAttribute(k,v){this[k]=v;}}), sidebarToggle=button(),sidebarRailToggle=button();
  for(const collapsed of [true,false]) {
    run('syncSidebar',{sidebarToggle,sidebarRailToggle,document:{body:{classList:{contains:()=>collapsed}}}});
    for(const target of [sidebarToggle,sidebarRailToggle]) {
      assert.equal(target['aria-expanded'],String(!collapsed));
      assert.equal(target['aria-label'],collapsed?'Expand sidebar':'Collapse sidebar');
    }
  }
});

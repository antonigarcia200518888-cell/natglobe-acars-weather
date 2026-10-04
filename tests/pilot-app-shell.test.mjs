import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../public/pilot-app-shell.js', import.meta.url), 'utf8');

function harness({ standalone=false, flight={id:'LOCAL-TEST',dep:'EFHV',arr:'EFHN'} }={}) {
  const nodes=new Map();
  const classes=new Set();
  const events={};
  const themeButtons=['day','night'].map(theme=>({dataset:{appTheme:theme},attributes:{},events:{},setAttribute(k,v){this.attributes[k]=v;},addEventListener(k,v){this.events[k]=v;}}));
  const getNode=id=>{
    if(!nodes.has(id)) nodes.set(id,{dataset:{},attributes:{},events:{},hidden:false,disabled:false,open:false,textContent:'',returnValue:'',setAttribute(k,v){this.attributes[k]=v;},addEventListener(k,v){this.events[k]=v;},showModal(){this.open=true;},close(){if(this.open){this.open=false;this.events.close?.();}}});
    return nodes.get(id);
  };
  const scroller={scrollTop:0,scrollTo({top}){this.scrollTop=top;}};
  const location={pathname:'/booking-ops',search:'',hash:'',reloadCount:0,reload(){this.reloadCount++;}};
  const history={state:null,pushes:[],replaces:[],backs:0,replaceState(state,_,url){this.state=state;this.replaces.push(state);location.hash=url.includes('#')?'#'+url.split('#')[1]:'';},pushState(state,_,url){this.state=state;this.pushes.push(state);location.hash=url.includes('#')?'#'+url.split('#')[1]:'';},back(){this.backs++;}};
  const document={title:'',getElementById:getNode,querySelector:()=>scroller,querySelectorAll:()=>themeButtons,documentElement:{dataset:{efbTheme:'night'}},body:{dataset:{},classList:{contains:c=>classes.has(c),toggle(c,v){v?classes.add(c):classes.delete(c);}}}};
  const media={matches:standalone,addEventListener:()=>{}};
  const window={matchMedia:()=>media,requestAnimationFrame:fn=>fn(),addEventListener:(name,fn)=>{events[name]=fn;}};
  const navigator={onLine:true};
  const ctx=vm.createContext({window,document,history,location,navigator,Map,Date,Math});
  vm.runInContext(source,ctx);
  const visits=[];
  let shell;
  shell=window.createPilotAppShell({navigate:(view,options)=>{visits.push({view,options});shell.leave(view);shell.enter(view,options);},getFlight:()=>flight,setTheme:theme=>{document.documentElement.dataset.efbTheme=theme;shell.syncTheme(theme);},canInstall:()=>false,install:async()=>{}});
  return {shell,nodes,getNode,history,location,events,scroller,document,navigator,classes,themeButtons,visits};
}

test('workspace navigation creates app history, not duplicate entries on repeated taps',()=>{
  const h=harness();
  h.shell.enter('dashboard');
  assert.equal(h.history.replaces.length,1);
  assert.equal(h.getNode('appBackBtn').disabled,true);
  h.shell.leave('weather'); h.shell.enter('weather');
  h.shell.enter('weather');
  assert.equal(h.history.pushes.length,1);
  assert.equal(h.history.state.ngaAppDepth,1);
  assert.equal(h.getNode('appBackBtn').disabled,false);
  h.getNode('appBackBtn').events.click();
  assert.equal(h.history.backs,1);
  assert.equal(h.document.title,'Weather · NGA Pilot EFB');
});

test('Back and hash navigation restore each workspace scroll without pushing again',()=>{
  const h=harness();
  h.shell.enter('dashboard');
  const homeState=h.history.state;
  h.scroller.scrollTop=680;
  h.shell.leave('documents');h.shell.enter('documents');
  assert.equal(h.scroller.scrollTop,0);
  h.location.hash='';h.history.state=homeState;h.events.popstate();
  assert.equal(h.scroller.scrollTop,680);
  assert.equal(h.visits.at(-1).options.historyMode,'none');
  assert.equal(h.history.pushes.length,1);
  h.events.hashchange();
  assert.equal(h.visits.length,1,'popstate and hashchange must not navigate twice');
});

test('focus mode is reversible and only available for an actual flight briefing',()=>{
  const h=harness();h.shell.enter('flight-plan');
  h.getNode('appFocusBtn').events.click();
  assert.equal(h.classes.has('app-focus'),true);
  assert.equal(h.getNode('appFocusLabel').textContent,'Exit focus');
  h.shell.leave('flights');h.shell.enter('flights');
  assert.equal(h.classes.has('app-focus'),false);
  assert.equal(h.getNode('appFocusBtn').hidden,true);
  const empty=harness({flight:null});empty.shell.enter('flight-plan');empty.getNode('appFocusBtn').events.click();
  assert.equal(empty.classes.has('app-focus'),false);
});

test('settings reflects theme and standalone state, and reload requires explicit confirmation',()=>{
  const h=harness({standalone:true});h.shell.openSettings();
  assert.equal(h.getNode('appDisplayState').textContent,'Home Screen app');
  assert.equal(h.getNode('appInstallGuide').hidden,true);
  assert.equal(h.getNode('appNativeInstallBtn').hidden,true);
  h.themeButtons[0].events.click();
  assert.equal(h.document.documentElement.dataset.efbTheme,'day');
  assert.equal(h.themeButtons[0].attributes['aria-pressed'],'true');
  h.shell.confirmReload();
  assert.equal(h.getNode('appPreferencesDialog').open,false);
  assert.equal(h.getNode('appReloadDialog').open,true);
  h.getNode('appReloadDialog').close();
  assert.equal(h.location.reloadCount,0);
  h.shell.confirmReload();h.getNode('appReloadDialog').returnValue='reload';h.getNode('appReloadDialog').close();
  assert.equal(h.location.reloadCount,1);
});

test('offline indicator does not imply current briefing data or successful server sync',()=>{
  const h=harness();h.navigator.onLine=false;h.events.offline();
  assert.equal(h.getNode('appConnection').textContent,'Offline');
  assert.match(h.getNode('appNetworkDetail').textContent,/stale/);
  h.navigator.onLine=true;h.events.online();
  assert.equal(h.getNode('appConnection').textContent,'Network');
  assert.match(h.getNode('appNetworkDetail').textContent,/freshness/);
});

test('app resources are versioned together and Home Screen metadata is present',()=>{
  const html=fs.readFileSync(new URL('../views/booking-ops.html',import.meta.url),'utf8');
  const sw=fs.readFileSync(new URL('../public/pilot-sw.js',import.meta.url),'utf8');
  const version=sw.match(/PILOT_STYLESHEET_VERSION = '([^']+)'/)[1];
  for(const ext of ['js','css']) assert.ok(html.includes(`/pilot-app-shell.${ext}?v=${version}`));
  assert.match(sw,/url\.pathname === '\/pilot-app-shell\.js'/);
  for(const file of ['booking-ops.html','pilot-login.html']) {
    const page=fs.readFileSync(new URL('../views/'+file,import.meta.url),'utf8');
    assert.match(page,/apple-mobile-web-app-capable" content="yes/);
    assert.match(page,/rel="apple-touch-icon"/);
  }
  const manifest=JSON.parse(fs.readFileSync(new URL('../public/pilot-manifest.webmanifest',import.meta.url),'utf8'));
  assert.equal(manifest.display,'standalone');
  assert.equal(manifest.id,manifest.start_url);
});

// Opt-in browser smoke test against a disposable local Pilot Ops server.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import fs from 'node:fs/promises';
import path from 'node:path';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.PILOT_QA_PLAYWRIGHT);
const base=process.env.PILOT_QA_URL||'http://localhost:3134';
assert.ok(['localhost','127.0.0.1'].includes(new URL(base).hostname));
const output=process.env.PILOT_QA_SCREENSHOTS;assert.ok(output);await fs.mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true,channel:'chrome'});
const context=await browser.newContext({viewport:{width:1194,height:834},locale:'en-US',serviceWorkers:'block'});
async function get(route){const r=await context.request.get(base+route);assert.equal(r.status(),200);return r.json();}
const login=await context.request.post(base+'/api/pilot-login',{data:{code:'NGA-LOCAL-QA-ONLY'}});assert.equal(login.status(),200);
const catalogue=await get('/api/booking-ops/airport-catalogue');assert.ok(catalogue.airports.some(a=>a.icao==='EGLL'));assert.ok(catalogue.airports.length>1000);
const hv=await get('/api/booking-ops/airport-reference?icao=EFHV');assert.ok(hv.runways.length);assert.ok(hv.directory.page);assert.ok(hv.directory.comMhz.includes('122.500'));
assert.equal(hv.fuel.status,'listed');const avgas=hv.fuel.prices.find(p=>p.grade==='AVGAS 100LL');assert.ok(avgas.eurPerL>0);
const nu=await get('/api/booking-ops/weather-report?icao=EFNU&product=summary');assert.ok(nu.efnuLocal.observedAt);assert.equal(nu.efnuLocal.frequencyMhz,'135.825');assert.ok(nu.reports.every(r=>r.icao==='EFNU'));
const efhn=await get('/api/booking-ops/airport-reference?icao=EFHN&section=fuel');assert.deepEqual(efhn.prices,[]);
const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
await page.addInitScript(()=>sessionStorage.setItem('nga-pilot-login-handoff',String(Date.now())));
const shot=name=>page.screenshot({path:path.join(output,name+'.png'),animations:'disabled'});
async function overflow(){assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>document.documentElement.clientWidth+1),false);}
try{
  await page.goto(base+'/booking-ops#airports',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>document.body.dataset.appWorkspace==='airports'&&!window.NGAPilotLock.locked);
  await page.getByRole('heading',{name:'Communications & references'}).waitFor();
  assert.equal(await page.locator('#airportFileCode').innerText(),'EFHV');
  await page.locator('#airportFavoriteBtn').click();
  await page.locator('[data-airport-list="favorites"]').click();
  assert.match(await page.locator('#airportResults').innerText(),/EFHV/);
  await page.locator('[data-airport-view="runways"]').click();await page.locator('.airport-runway-list article').first().waitFor();
  await overflow();await shot('airport-runways-landscape');
  await page.locator('[data-airport-view="fuel"]').click();
  await page.locator('.airport-fuel-row').filter({hasText:'AVGAS 100LL'}).getByRole('button',{name:'Use in calculator'}).click();
  await page.locator('#fuelCalcAmount').fill('25');await page.locator('#fuelCalcDensity').fill('0,72');
  assert.equal(await page.locator('#fuelCalcAirport').inputValue(),'EFHV');assert.equal(Number(await page.locator('#fuelCalcPrice').inputValue()),avgas.eurPerL);
  assert.match(await page.locator('#fuelCalcOut-kg').innerText(),/^18 kg$/);
  assert.equal(await page.locator('#fuelCalcCost').innerText(),new Intl.NumberFormat('en-US',{style:'currency',currency:'EUR'}).format(25*avgas.eurPerL));
  await overflow();await shot('fuel-calculator-landscape');
  await page.locator('#fuelCalcAirport').fill('EFHN');assert.equal(await page.locator('#fuelCalcPrice').inputValue(),'');
  await page.locator('#fuelCalcLookup').click();await page.getByText('No published supplier price for this airport.',{exact:false}).first().waitFor();
  assert.equal(await page.locator('#fuelCalcApply').isDisabled(),true);
  await page.locator('#pilotWorkspaceNav [data-suite-view="airports"]').click();
  await page.locator('#airportFind').fill('Nummela');await page.locator('#airportResults button').filter({hasText:'EFNU'}).click();
  await page.locator('[data-airport-view="weather"]').click();await page.locator('#airportFileContent .efnu-local').waitFor();
  assert.match(await page.locator('#airportFileContent .efnu-local').innerText(),/135\.825 MHz.*Not an ATC clearance/);
  if(nu.efnuLocal.status==='observed'){
    await page.locator('#airportFileContent summary').filter({hasText:'Read published broadcast'}).click();
    assert.match(await page.locator('#airportFileContent .efnu-local pre').innerText(),/NUMMELA/);
    if(nu.efnuLocal.audioUrl){
      const audio=page.locator('#airportFileContent audio');assert.match(await audio.getAttribute('src'),/^https:\/\/info\.efnu\.fi\/mp3\//);
      await audio.evaluate(async el=>{el.muted=true;await el.play();el.pause();});
    }
  }
  await shot('airport-efnu-atis-landscape');
  await page.setViewportSize({width:834,height:1194});await page.locator('.efb-content').evaluate(el=>{el.scrollTop=0;});await overflow();await shot('airport-efnu-atis-portrait');
  await page.setViewportSize({width:390,height:844});await overflow();
  await page.locator('#pilotWorkspaceNav [data-suite-view="weather"]').click();
  await page.locator('#weatherDep').fill('EFNU');await page.locator('#weatherArr').fill('EFHV');await page.locator('#gatherWeatherBtn').click();
  await page.locator('#efnuWeatherPanel .efnu-local').waitFor({timeout:30000});
  assert.equal(await page.locator('#efhvWeatherPanel').isVisible(),true);
  await overflow();assert.deepEqual(errors,[]);
  console.log(JSON.stringify({ok:true,efhvFuelGrades:hv.fuel.prices.length,efnu:nu.efnuLocal.status,audioAvailable:Boolean(nu.efnuLocal.audioUrl),screenshots:output}));
}catch(error){await shot('airport-failure');console.error(errors);throw error;}
finally{await browser.close();}

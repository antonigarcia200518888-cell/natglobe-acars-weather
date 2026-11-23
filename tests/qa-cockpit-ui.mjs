// Opt-in, localhost-only browser QA. No production or private pilot records.
// Requires PILOT_QA_PLAYWRIGHT (package path) and PILOT_QA_SCREENSHOTS (directory).
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import fs from 'node:fs/promises';
import path from 'node:path';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.PILOT_QA_PLAYWRIGHT);
const base=process.env.PILOT_QA_URL || 'http://localhost:3134';
assert.ok(['localhost','127.0.0.1'].includes(new URL(base).hostname));
const output=process.env.PILOT_QA_SCREENSHOTS;assert.ok(output);await fs.mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true,channel:'chrome'});
const context=await browser.newContext({viewport:{width:1194,height:834},deviceScaleFactor:1,serviceWorkers:'block'});
const login=await context.request.post(base+'/api/pilot-login',{data:{code:'NGA-LOCAL-QA-ONLY'}});assert.equal(login.status(),200);
const created=await context.request.post(base+'/api/booking-ops/manual-flight',{data:{dep:'EFHV',arr:'EFHN',flightRules:'VFR',requestDate:'2026-11-19',requestTime:'12:00',estimatedFlightMinutes:40,title:'LOCAL QA cockpit layout'}});
assert.equal(created.status(),201);const record=(await created.json()).request;
const page=await context.newPage(), errors=[];
page.on('pageerror',e=>errors.push(e.message));
await page.addInitScript(()=>sessionStorage.setItem('nga-pilot-login-handoff',String(Date.now())));
try {
  await page.goto(base+'/booking-ops#dashboard',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>window.PilotFlights && !window.NGAPilotLock.locked);
  await page.evaluate(record=>window.PilotFlights.activate(record),record);
  async function shot(name){await page.screenshot({path:path.join(output,name+'.png'),animations:'disabled'});}
  async function overflow(root=page){
    const dimensions=await root.evaluate(()=>({w:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth}));
    assert.ok(dimensions.scroll<=dimensions.w+1,JSON.stringify(dimensions));
  }
  await overflow();await shot('cockpit-map-landscape');
  await page.locator('#pilotWorkspaceNav [data-suite-view="flight-plan"]').click();
  const frame=await (await page.locator('#ofpOpsFrame').elementHandle()).contentFrame();
  await frame.waitForFunction(()=>window.ngaOFP && !document.getElementById('saveState').classList.contains('loading'));
  assert.equal(await frame.locator('.ofp-workspace-button').count(),6);
  for(const [id,name] of [['Setup','overview'],['Route','route'],['Time','schedule'],['Load','fuel'],['Brief','review']]){
    await frame.locator('#taskTab'+id).click();await overflow(frame);await shot('ofp-'+name+'-landscape');
    const overlaps=await frame.evaluate(()=>{
      const footer=document.querySelector('.ofp-command-bar').getBoundingClientRect(),area=document.querySelector('.ofp-app-workspace').getBoundingClientRect();
      return area.bottom>footer.top+1;
    });assert.equal(overlaps,false,'OFP scroll area overlaps action bar');
  }
  await frame.locator('#taskTabFiles').click();
  await frame.locator('[data-kind="journey"]').click();
  await frame.locator('.flight-document-paper').waitFor();
  await frame.locator('[data-expand]').click();
  await frame.locator('.flight-document-dialog[open]').waitFor();await shot('journey-log-landscape');
  await page.keyboard.press('Escape');assert.equal(await frame.locator('.flight-document-dialog[open]').count(),0);
  await frame.locator('[data-close]').click();assert.equal(await frame.locator('.shell').getAttribute('data-workspace'),'SETUP');
  await page.locator('#pilotWorkspaceNav [data-suite-view="weather"]').click();
  await page.locator('#gatherWeatherBtn').click();
  await page.locator('#efhvWeatherPanel .wx-local-airfield').waitFor({timeout:30000});
  await overflow();
  assert.match(await page.locator('#efhvWeatherPanel').innerText(),/FMI forecast for .* UTC/);
  assert.match(await page.locator('#efhvWeatherPanel').innerText(),/Not METAR or TAF/);
  await shot('weather-efhv-landscape');
  await page.locator('#weatherDep').fill('EFHK');await page.locator('#weatherArr').fill('EFTP');
  assert.equal(await page.locator('#efhvWeatherPanel').isVisible(),false);
  await page.locator('#gatherWeatherBtn').click();
  await page.waitForFunction(()=>document.getElementById('weatherCategoryStrip').children.length>0,{},{timeout:35000});
  assert.equal(await page.locator('#activeFlightWeather').innerText(),'NOT BRIEFED','manual-airport weather must not be attached to a different selected flight');
  await page.locator('#weatherAirportsTab').click();await overflow();await shot('weather-airports-landscape');
  await page.locator('#weatherRequestsTab').click();await page.locator('#wxRequestIcao').fill('EFHK');
  await page.locator('#wxRequestSend').click();await page.locator('.wx-message').first().waitFor();await shot('weather-requests-landscape');
  await page.locator('.wx-message [aria-pressed="false"]').first().click();
  assert.match(await page.locator('.wx-message-body').first().innerText(),/Wind:|No METAR/);
  await page.locator('#refreshWeatherBtn').click();
  await page.waitForFunction(()=>!document.getElementById('openWeatherPdfBtn').disabled,{},{timeout:35000});
  await page.locator('#openWeatherPdfBtn').click();
  await page.locator('#wxReaderPages .wx-paper-page').first().waitFor({timeout:30000});
  await shot('weather-document-return');
  await page.locator('#wxReaderMode').click();await page.locator('#wxReaderPages .wx-reading-page').first().waitFor();
  await page.locator('#wxReaderBack').click();await page.locator('#wxReader').waitFor({state:'hidden'});
  await page.locator('#weatherRadarTab').click();
  await page.waitForFunction(()=>/latest \d+ min old|UNAVAILABLE/.test(document.getElementById('weatherRadarTime').textContent),{},{timeout:25000});
  console.log('Radar:',await page.locator('#weatherRadarTime').innerText());
  assert.match(await page.locator('#weatherRadarTime').innerText(),/latest \d+ min old/,'FMI frame must actually load, not just show a loading timestamp');
  await page.locator('#weatherRadarBackBtn').click();
  await page.waitForFunction(()=>/latest \d+ min old/.test(document.getElementById('weatherRadarTime').textContent),{},{timeout:16000});
  await page.locator('#weatherRadarPlayBtn').click();
  await page.waitForFunction(()=>document.getElementById('weatherRadarPlayBtn').getAttribute('aria-pressed')==='false',{},{timeout:16000});
  assert.equal(await page.locator('#weatherRadarTimeline').inputValue(),await page.locator('#weatherRadarTimeline').getAttribute('max'),'Replay ends on the latest frame');
  await shot('weather-radar-landscape');
  await page.setViewportSize({width:1024,height:768});await overflow();await shot('weather-radar-small-ipad');
  await page.setViewportSize({width:834,height:1194});await page.locator('#weatherRequestsTab').click();await overflow();await shot('weather-requests-portrait');
  await page.setViewportSize({width:1440,height:900});
  await page.evaluate(()=>{location.hash='aircraft';});await page.locator('#aircraftOpsPane').waitFor();await overflow();await shot('aircraft-hangar-desktop');
  await page.evaluate(()=>{location.hash='flights';});await page.locator('#bookingsWorkspace').waitFor();await overflow();await shot('flights-desktop');
  for(const size of [{width:1194,height:834},{width:1024,height:768},{width:834,height:1194}]){
    await page.setViewportSize(size);
    for(const view of ['flights','flight-plan','aircraft','performance','documents','logbook']){
      await page.evaluate(view=>{location.hash=view;},view);
      await page.waitForFunction(view=>document.body.dataset.appWorkspace===view,view);
      await page.waitForTimeout(100);await overflow();
      if(view==='flight-plan'){
        if(!await frame.locator('#taskTabSetup').isVisible())await frame.locator('#ofpSectionsToggle').click();
        await frame.locator('#taskTabSetup').click();
        const alternate=await frame.locator('input[data-field="alternate"]').boundingBox();
        assert.ok(alternate.width>=100,'Alternate airport entry must not collapse into a clipped field');
      }
      await shot(view+'-'+size.width+'x'+size.height);
    }
  }
  await page.setViewportSize({width:1194,height:834});
  await page.evaluate(()=>{location.hash='dashboard';});
  await context.grantPermissions(['geolocation'],{origin:base});
  await context.setGeolocation({latitude:60.317,longitude:24.963,accuracy:18});
  await page.locator('#cockpitLocationBtn').click();await page.locator('#cockpitLocationStart').click();
  await page.waitForFunction(()=>document.getElementById('cockpitPositionDetail').textContent.includes('±18 m'));
  await shot('device-position-details');
  await page.locator('#cockpitLocationCenter').click();
  await page.locator('#cockpitLocationDialog').waitFor({state:'hidden'});await shot('device-position-map');
  // Use the app's actual theme control so nested OFP state is updated too.
  await page.evaluate(()=>document.querySelector('[data-app-theme="day"]').click());
  assert.equal(await page.locator('html').getAttribute('data-efb-theme'),'day');
  await page.evaluate(()=>{location.hash='weather';});
  await page.locator('#weatherAirportsTab').click();await shot('weather-airports-day');
  const contrast=await page.locator('#weatherOpsPane button[aria-pressed="true"]').first().evaluate(el=>{
    const style=getComputedStyle(el);return {text:style.color,background:style.backgroundColor};
  });
  assert.notEqual(contrast.text,contrast.background,'Selected weather controls need visible day-mode labels');
  await page.evaluate(()=>{location.hash='flight-plan';});
  await page.waitForFunction(()=>document.body.dataset.appWorkspace==='flight-plan');
  if(!await frame.locator('#taskTabSetup').isVisible())await frame.locator('#ofpSectionsToggle').click();
  await frame.locator('#taskTabSetup').click();await overflow(frame);await shot('ofp-overview-day');
  assert.deepEqual(errors,[],'Unexpected browser runtime errors');
  console.log(JSON.stringify({ok:true,flight:record.id,screenshots:output,viewports:['1194×834','1024×768','834×1194','1440×900'],errors}));
} catch(error){await page.screenshot({path:path.join(output,'failure.png')});console.error('Browser errors:',errors);throw error;}
finally{await browser.close();}

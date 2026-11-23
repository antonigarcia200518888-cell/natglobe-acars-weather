import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createAirportSources,parseAirfieldDirectory,parseEfnuInformation,parseFuelDirectory} from '../lib/airport-sources.js';

const now=Date.parse('2026-11-23T19:41:00Z');
const epoch=Date.parse('2026-11-23T19:40:00Z')/1000;
const listings={timeStamp:{timeStamp:'2026-11-23 21:40:52'},validFrom:'2026-10-01',ageMinutes:1,airportList:[
  {airportCode:'EFHV',pouserStatus:'OK',fuelPrices:{'AVGAS 100LL':'3.828','JET A-1':'2,008',MOGAS:'NA',OTHER:'3.0'}},
  {airportCode:'EFNU',pouserStatus:'closed',fuelPrices:{'AVGAS 100LL':'3.123'}},
  {airportCode:'EFHN',pouserStatus:'-',fuelPrices:{MOGAS:'NA'}}
]};
const broadcast={states:[{timestamp:epoch,mp3:'mp3/atis-efnu-231940Z-1795462850.mp3',message:['NUMMELA INFO UNIFORM.'],report:{datetime_unix:epoch-5,repid:'U',wind_dir:350,wind_kt:10,wind_gust_kt:18,temperature:0.1,dewpoint:-2,vis_km:10,qnh:1011.5,clouds:[['BKN',13,''],['OVC',28,'']]}}]};

test('fuel directory keeps grade, exact airport, currency, timestamps and pump status separate',()=>{
  const data=parseFuelDirectory(listings,now);
  assert.equal(data.get('EFHV').status,'listed');
  assert.deepEqual(data.get('EFHV').prices.map(p=>[p.grade,p.eurPerL]),[['AVGAS 100LL',3.828],['JET A-1',2.008]]);
  assert.equal(data.get('EFHV').updatedAt,'2026-11-23T19:40:52.000Z');
  assert.equal(data.get('EFHV').date,'2026-10-01');
  assert.equal(data.get('EFNU').status,'closed');assert.deepEqual(data.get('EFNU').prices,[]);
  assert.equal(data.get('EFHN').status,'missing');assert.equal(data.has('EFRY'),false);
  assert.equal(parseFuelDirectory(listings,now+8*86400000).get('EFHV').status,'stale');
  assert.equal(parseFuelDirectory({...listings,ageMinutes:null},now).get('EFHV').status,'stale');
});

test('airport directory only uses the verified column layout and safe directory links',()=>{
  const cells=['AD','ICAO ID','','','','','','COM FREQ','','ACC FRQ'];
  const row=['Hyvinkää','EFHV','','','','','','122.500','','127.425'];
  const table='<table><thead><tr>'+cells.map(s=>`<th>${s}</th>`).join('')+'</tr></thead><tbody><tr>'+row.map(s=>`<td>${s}</td>`).join('')+'</tr></tbody></table>';
  const data=parseAirfieldDirectory('<a href="https://lentopaikat.fi/en/hyvinkaa-efhv/">Hyvinkää – EFHV</a><a href="https://example.org/en/evil/">EFNU</a>'+table);
  assert.deepEqual(data.rows.get('EFHV').comMhz,['122.500']);assert.deepEqual(data.rows.get('EFHV').accMhz,['127.425']);
  assert.equal(data.links.get('EFHV'),'https://lentopaikat.fi/en/hyvinkaa-efhv/');assert.equal(data.links.has('EFNU'),false);
  assert.equal(parseAirfieldDirectory(table.replace('COM FREQ','NOT COM')).rows.size,0);
});

test('EFNU separates timestamped sensor data, unchanged transcript and allowed audio',()=>{
  const info=parseEfnuInformation(broadcast,now);
  assert.equal(info.status,'observed');assert.equal(info.identifier,'U');assert.equal(info.frequencyMhz,'135.825');
  assert.deepEqual(info.clouds,[{amount:'BKN',heightFt:1300},{amount:'OVC',heightFt:2800}]);
  assert.equal(info.message[0],'NUMMELA INFO UNIFORM.');assert.match(info.audioUrl,/^https:\/\/info\.efnu\.fi\/mp3\//);
  const old=parseEfnuInformation(broadcast,now+31*60000);assert.equal(old.status,'stale');assert.equal(old.audioUrl,null);
  const future=parseEfnuInformation(broadcast,now-3*60000);assert.equal(future.status,'stale');assert.equal(future.audioUrl,null);
  const different=structuredClone(broadcast);different.states[0].mp3='//evil.test/x.mp3';assert.equal(parseEfnuInformation(different,now).audioUrl,null);
  assert.equal(parseEfnuInformation({states:[]},now).status,'missing');
});

test('provider queries share a cached feed without fabricating a missing airport price',async()=>{
  let calls=0;const sources=createAirportSources({now:()=>now,fetchImpl:async url=>{calls++;return new Response(JSON.stringify(url.includes('atis-state')?broadcast:listings));}});
  const [a,b]=await Promise.all([sources.fuelFor('EFHV'),sources.fuelFor('EFNU')]);
  assert.equal(calls,1);assert.equal(a.status,'listed');assert.equal(b.status,'closed');
  assert.equal((await sources.fuelFor('EGLL')).status,'unsupported');assert.equal((await sources.fuelFor('EFTP')).status,'missing');assert.equal(calls,1);
  await Promise.all([sources.efnu(),sources.efnu()]);assert.equal(calls,2);
});

test('directory follows only the same-page WordPress cron redirect',async()=>{
  let calls=0;
  const get=location=>createAirportSources({now:()=>now,fetchImpl:async url=>{
    calls++;
    return url.includes('doing_wp_cron')?new Response('<a href="https://lentopaikat.fi/en/hyvinkaa-efhv/">Hyvinkää – EFHV</a>'):new Response(null,{status:302,headers:{Location:location}});
  }});
  assert.match((await get('/en/uncontrolled-airport-summary/?doing_wp_cron=1795463613.294').directoryFor('EFHV')).page,/hyvinkaa-efhv/);
  assert.equal(calls,2);
  assert.equal(await get('https://example.test/en/uncontrolled-airport-summary/?doing_wp_cron=1795463613.294').directoryFor('EFHV'),null);
  assert.equal(calls,3);
});

test('fuel calculator distinguishes gallons and can convert within a unit family without density',()=>{
  const read=name=>fs.readFileSync(new URL('../public/'+name,import.meta.url),'utf8');
  new vm.Script(read('pilot-airport-workspace.js'));
  const ctx=vm.createContext({});vm.runInContext(read('pilot-fuel-calculator.js'),ctx);
  const calc=ctx.PilotFuel.calculate;
  assert.equal(calc({unit:'usg',amount:'10',price:'3,828'}).litres,37.85411784);
  assert.equal(calc({unit:'imp',amount:'1'}).litres,4.54609);
  assert.equal(calc({unit:'lb',amount:'1'}).kg,0.45359237);
  assert.equal(calc({unit:'lb',amount:'1'}).litres,null);
  assert.equal(calc({unit:'l',amount:'25',density:'0,72',price:'3.00'}).cost,75);
  assert.equal(calc({unit:'l',amount:'25',density:'0,72'}).kg,18);
  const adjusted=calc({unit:'l',amount:'100',density:'0.8',correction:true,temp:'25',referenceTemp:'15',coefficient:'0.001'});
  assert.ok(Math.abs(adjusted.kg-80/1.01)<1e-9);
  assert.equal(calc({unit:'l',amount:'100',density:'0.8',correction:true,temp:'25',referenceTemp:'15'}).kg,null);
  assert.equal(calc({unit:'l',amount:''}),null);assert.equal(calc({unit:'l',amount:'-2'}),null);
});

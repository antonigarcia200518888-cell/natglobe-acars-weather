import test from 'node:test';
import assert from 'node:assert/strict';
import {createEfhvWeather,parseEfhvPage} from '../lib/efhv-weather.js';

const forecastRow=(time,wind='015&deg; 14 (26)')=>`<tr><td>${time}</td><td>-1&deg;</td><td>79%, -2&deg;</td><td>${wind}</td><td>8/8</td><td>0</td></tr>`;
const page=(stamp,rows='')=>`<div id="saanyt"><p>Mitattu ${stamp}<br>Aurinko: 08:42–15:30<br></p></div>
<table><thead><tr><th>Kello</th><th>T</th><th>Hum, Dp</th><th>Tuuli (kt)</th><th>Pilvet</th><th>Sade<br>mm</th></tr></thead><tbody>${rows}</tbody></table>`;
const day=text=>`<tr><td colspan="6"><strong>${text}</strong></td></tr>`;
const now=Date.parse('2026-11-23T18:34:35Z');
const modified=Date.parse('2026-11-23T18:34:06Z');
const sample=page('23.11. 20:34',day('23.11.')+forecastRow('20:00')+forecastRow('21:00')+forecastRow('23:00','999&deg; 4'));
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jf1sAAAAASUVORK5CYII=','base64');
const response=(body,type,modified)=>new Response(body,{headers:{'Content-Type':type,'Last-Modified':new Date(modified).toUTCString()}});

test('EFHV publishes a UTC-translated sensor timestamp and separately typed forecast',()=>{
  assert.deepEqual(parseEfhvPage(sample,modified,now),{
    observedAt:'2026-11-23T18:34:00.000Z',
    forecasts:[{validAt:'2026-11-23T19:00:00.000Z',temperatureC:-1,direction:15,speedKt:14,gustKt:26}]
  });
});

test('EFHV resolves year rollover but rejects an ambiguous, impossible or unanchored local timestamp',()=>{
  const turn=Date.parse('2026-12-31T21:58:20Z');
  assert.equal(parseEfhvPage(page('31.12. 23:58',day('1.1.')+forecastRow('00:00')),turn,turn).forecasts[0].validAt,'2026-12-31T22:00:00.000Z');
  const autumn=Date.parse('2026-10-25T02:00:00Z');
  assert.equal(parseEfhvPage(page('25.10. 03:30'),autumn,autumn).observedAt,null);
  assert.equal(parseEfhvPage(page('31.2. 18:30'),now,now).observedAt,null);
  assert.equal(parseEfhvPage(sample,NaN,now).observedAt,null);
  assert.equal(parseEfhvPage(sample,modified,now+7*3600000).forecasts.length,0);
});

test('EFHV shares fetches, withholds an old graphic, and returns a verified PNG when updated',async()=>{
  let clock=now, calls=0, chartModified=now-30*86400000;
  const get=createEfhvWeather({now:()=>clock,fetchImpl:async url=>{
    calls++;
    return url.endsWith('.png')?response(png,'image/png',chartModified):response(sample,'text/html',modified);
  }});
  const [a,b]=await Promise.all([get(),get()]);
  assert.deepEqual(a,b);assert.equal(calls,2);assert.equal(a.windChart,null);assert.ok(a.forecast);
  assert.deepEqual(await get(),a);assert.equal(calls,2);
  chartModified=modified;clock+=61000;
  const fresh=await get();assert.equal(calls,4);assert.equal(fresh.windChart.png,png.toString('base64'));
  clock+=16*60000;
  assert.equal((await get()).windChart,null);assert.equal(calls,5,'no graphic request for an old observation');
});

test('EFHV network errors are isolated and short-cached',async()=>{
  let calls=0;
  const get=createEfhvWeather({now:()=>now,fetchImpl:async()=>{calls++;throw new Error('EFHV offline');}});
  const a=await get();assert.equal(a.unavailable,true);assert.equal(a.forecast,null);assert.equal(a.observedAt,null);
  assert.deepEqual(await get(),a);assert.equal(calls,1);
});

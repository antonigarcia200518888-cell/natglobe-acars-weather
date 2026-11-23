// Independent Hyvinkää station page. Never convert its data into METAR/TAF.
const SOURCE = 'https://efhv.fi/';
const MINUTE = 60000;
const HOUR = 60 * MINUTE;
const localClock = new Intl.DateTimeFormat('en-GB', {
  timeZone:'Europe/Helsinki', year:'numeric', month:'2-digit', day:'2-digit',
  hour:'2-digit', minute:'2-digit', hourCycle:'h23'
});

function localParts(time) {
  return Object.fromEntries(localClock.formatToParts(time).map(({type,value}) => [type,Number(value)]));
}

// The page omits the year and UTC offset. Resolve dates around the origin's
// Last-Modified timestamp, validating the wall clock against Helsinki time.
function localInstants(day, month, hour, minute, anchor) {
  if (day<1 || day>31 || month<1 || month>12 || hour>23 || minute>59) return [];
  const year = localParts(anchor).year;
  return [year-1,year,year+1].flatMap(y => [2,3].map(offset => Date.UTC(y,month-1,day,hour-offset,minute)))
    .filter(time => {
      if (Math.abs(time-anchor)>3*24*HOUR) return false;
      const parts=localParts(time);
      return parts.day===day && parts.month===month && parts.hour===hour && parts.minute===minute;
    });
}

const plain = value => value.replace(/<[^>]*>/g,'').replace(/&deg;|&#176;/gi,'°').replace(/&nbsp;/gi,' ').trim();
const iso = time => new Date(time).toISOString();

export function parseEfhvPage(html, modified, now = Date.now()) {
  if (!Number.isFinite(modified) || modified>now+MINUTE) return {observedAt:null, forecasts:[]};
  const observation = html.match(/<div\b[^>]*\bid=["']saanyt["'][^>]*>([\s\S]*?)<\/div>/i)?.[1] || '';
  const stamp = observation.match(/Mitattu\s+(\d{1,2})\.(\d{1,2})\.\s+(\d{2}):(\d{2})(?=\s|<)/i);
  const matches = stamp ? localInstants(...stamp.slice(1).map(Number),modified).filter(time => time<=modified && time<=now) : [];
  const observedAt = matches.length===1 ? iso(matches[0]) : null;
  const forecasts = [];
  if (now-modified>6*HOUR) return {observedAt,forecasts};
  const table = [...html.matchAll(/<table\b[^>]*>([\s\S]*?)<\/table>/gi)].map(match=>match[1]).find(markup => {
    const headers = [...markup.matchAll(/<th\b[^>]*>([\s\S]*?)<\/th>/gi)].map(match=>plain(match[1]));
    return headers.slice(0,4).join('|')==='Kello|T|Hum, Dp|Tuuli (kt)';
  });
  if (!table) return {observedAt,forecasts};
  let date;
  for (const row of table.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells=[...row[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map(match=>plain(match[1]));
    if(cells.length===1){date=cells[0].match(/^(\d{1,2})\.(\d{1,2})\.$/);continue;}
    if(!date || cells.length!==6) continue;
    const clock=cells[0].match(/^(\d{2}):(\d{2})$/), temp=cells[1].match(/^([+-]?\d{1,2})°$/);
    const wind=cells[3].match(/^(\d{1,3})°\s+(\d{1,3})(?:\s+\((\d{1,3})\))?$/);
    if(!clock || !temp || !wind || Number(wind[1])>360) continue;
    const valid = localInstants(+date[1],+date[2],+clock[1],+clock[2],modified);
    // Don't guess which occurrence of an ambiguous local DST time was meant.
    if(valid.length!==1 || valid[0]<now-MINUTE || valid[0]>now+48*HOUR) continue;
    forecasts.push({validAt:iso(valid[0]),temperatureC:+temp[1],direction:+wind[1],speedKt:+wind[2],gustKt:wind[3]===undefined?null:+wind[3]});
  }
  forecasts.sort((a,b)=>a.validAt.localeCompare(b.validAt));
  return {observedAt,forecasts};
}

async function readBounded(response, limit) {
  const chunks=[];
  let size=0;
  for await (const chunk of response.body) {
    size+=chunk.byteLength;
    if(size>limit) throw new Error('EFHV response too large');
    chunks.push(chunk);
  }
  return Buffer.concat(chunks,size);
}

export function createEfhvWeather({fetchImpl=fetch, now=Date.now}={}) {
  let cached, pending;
  const request = url => fetchImpl(url,{redirect:'error',cache:'no-cache',signal:AbortSignal.timeout(2400),headers:{'User-Agent':'NatGlobeAviation-EFB/2.0'}});
  async function refresh() {
    const page = await request(SOURCE);
    if(!page.ok || !page.headers.get('content-type')?.toLowerCase().startsWith('text/html')) {
      await page.body?.cancel();
      throw new Error('EFHV page unavailable');
    }
    const modified=Date.parse(page.headers.get('last-modified'));
    const parsed=parseEfhvPage((await readBounded(page,65536)).toString('utf8'),modified,now());
    let windChart=null;
    // Only fetch the wind chart when there is a recent sensor timestamp. A new
    // page does not mean the site's separately updated image is also new.
    if(parsed.observedAt && now()-Date.parse(parsed.observedAt)<=15*MINUTE) {
      try {
        const graphic=await request(SOURCE+'tuuli.png');
        const measured=Date.parse(parsed.observedAt), chartTime=Date.parse(graphic.headers.get('last-modified'));
        if(graphic.ok && graphic.headers.get('content-type')?.toLowerCase().split(';')[0]==='image/png'
          && chartTime>=measured-MINUTE && chartTime<=measured+5*MINUTE && chartTime<=now()+MINUTE) {
          const png=await readBounded(graphic,160*1024);
          if(png.length>=33 && png.subarray(0,16).equals(Buffer.from('89504e470d0a1a0a0000000d49484452','hex'))
            && [png.readUInt32BE(16),png.readUInt32BE(20)].every(n=>n>0 && n<=2048)) {
            windChart={updatedAt:iso(chartTime),png:png.toString('base64')};
          }
        } else await graphic.body?.cancel();
      } catch { /* Missing graphic does not invalidate the forecast or official reports. */ }
    }
    return {...parsed,windChart,checkedAt:iso(now())};
  }
  return async () => {
    if(!cached || cached.expires<=now()) {
      pending ??= refresh()
        .then(value => {cached={value,expires:now()+60000};})
        .catch(() => {cached={value:{observedAt:null,forecasts:[],windChart:null,checkedAt:iso(now()),unavailable:true},expires:now()+30000};})
        .finally(() => {pending=null;});
      await pending;
    }
    const {forecasts,...value}=cached.value;
    return {...value,forecast:forecasts.find(row=>Date.parse(row.validAt)>=now()) || null};
  };
}

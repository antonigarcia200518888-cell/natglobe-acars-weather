const FUEL_FEED='https://lentopaikat.fi/kananen/fuelprices.php';
const DIRECTORY='https://lentopaikat.fi/en/uncontrolled-airport-summary/';
export const FUEL_SOURCE='https://lentopaikat.fi/en/fuel-availability/';
export const FUEL_SUPPLIER='https://www.kanair.fi/category/10/ilmailupolttoaineet--aviation-fuel';
export const EFNU_SOURCE='https://info.efnu.fi/';
const EFNU_FEED='https://78sy389733.execute-api.eu-west-1.amazonaws.com/dev/0-atis-state?site=efnu';
const minute=60000, grades=['AVGAS 100LL','JET A-1','MOGAS'];
const icaoPattern=/^[A-Z]{4}$/;
const utc=ms=>new Date(ms).toISOString();
const localTime=new Intl.DateTimeFormat('sv-SE',{timeZone:'Europe/Helsinki',dateStyle:'short',timeStyle:'medium',hourCycle:'h23'});

function finnishTimestamp(value){
  if(typeof value!=='string'||!/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/.test(value))return NaN;
  const naive=Date.parse(value.replace(' ','T')+'Z');
  if(!Number.isFinite(naive))return NaN;
  const candidates=[2,3].map(hours=>naive-hours*60*minute).filter(ms=>localTime.format(ms).replace(',','')===value);
  return candidates.length===1?candidates[0]:NaN;
}

export function parseFuelDirectory(payload, now=Date.now()){
  if(!Array.isArray(payload?.airportList))throw new Error('Invalid Lentopaikat price feed');
  const updated=finnishTimestamp(payload.timeStamp?.timeStamp);
  const date=/^\d{4}-\d{2}-\d{2}$/.test(payload.validFrom||'')?payload.validFrom:null;
  const stale=payload.stale===true || !Number.isFinite(payload.ageMinutes) || payload.ageMinutes<0 || payload.ageMinutes>7*24*60
    || !Number.isFinite(updated) || updated>now+5*minute || now-updated>7*24*60*minute;
  const records=new Map();
  for(const entry of payload.airportList){
    const code=String(entry?.airportCode||'').toUpperCase();
    if(!icaoPattern.test(code)||records.has(code))continue;
    const station=entry.pouserStatus==='OK'?'listed':String(entry.pouserStatus||'').toLowerCase()==='closed'?'closed':'unknown';
    const prices=grades.flatMap(grade=>{
      const raw=String(entry.fuelPrices?.[grade]??'').trim();
      const eurPerL=/^\d{1,2}(?:[.,]\d{1,4})?$/.test(raw)?Number(raw.replace(',','.')):NaN;
      return eurPerL>0?[{grade,eurPerL,date,source:FUEL_SOURCE}]:[];
    });
    records.set(code,{status:stale?'stale':station==='closed'?'closed':station==='listed'&&prices.length?'listed':'missing',
      prices:station==='listed'?prices:[],date,updatedAt:Number.isFinite(updated)?utc(updated):null,supplier:FUEL_SUPPLIER,source:FUEL_SOURCE});
  }
  return records;
}

const plain=html=>html.replace(/<[^>]*>/g,'').replace(/&nbsp;|&#160;/gi,' ').trim();
const freq=value=>/^1[123]\d\.\d{3}$/.test(value)?value:null;
export function parseAirfieldDirectory(html){
  const links=new Map(), rows=new Map();
  for(const link of html.matchAll(/<a\b[^>]*\bhref=["'](https:\/\/lentopaikat\.fi\/en\/[\w-]+\/)["'][^>]*>([^<]{1,160})<\/a>/gi)){
    const code=link[2].match(/\bEF[A-Z]{2}\b/g);
    if(code?.length===1 && !links.has(code[0]))links.set(code[0],link[1]);
  }
  for(const table of html.matchAll(/<table\b[^>]*>([\s\S]*?)<\/table>/gi)){
    const header=table[1].match(/<thead\b[^>]*>([\s\S]*?)<\/thead>/i)?.[1];
    const cols=header?[...header.matchAll(/<t[hd]\b[^>]*>([\s\S]*?)<\/t[hd]>/gi)].map(m=>plain(m[1])):[];
    if(cols[1]!=='ICAO ID'||cols[7]!=='COM FREQ'||cols[9]!=='ACC FRQ')continue;
    for(const row of table[1].matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)){
      const c=[...row[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map(m=>plain(m[1]));
      if(!/^EF[A-Z]{2}$/.test(c[1]||'')||c.length!==10)continue;
      const prior=rows.get(c[1])||{comMhz:[],accMhz:[],source:DIRECTORY};
      for(const [index,key] of [[7,'comMhz'],[9,'accMhz']]){const f=freq(c[index]);if(f&&!prior[key].includes(f))prior[key].push(f);}
      rows.set(c[1],prior);
    }
  }
  return {links,rows};
}

const range=(n,min,max)=>Number.isFinite(n)&&n>=min&&n<=max?n:null;
const textLines=(list,count,length)=>Array.isArray(list)?list.filter(item=>typeof item==='string').slice(0,count).map(item=>item.replace(/[\u0000-\u001f\u007f]/g,' ').trim().slice(0,length)).filter(Boolean):[];
export function parseEfnuInformation(payload,now=Date.now()){
  if(!Array.isArray(payload?.states))throw new Error('Invalid EFNU feed');
  const state=payload.states[0];
  const r=state?.report;
  const epoch=range(r?.datetime_unix,946684800,4102444800);
  const issued=range(state?.timestamp,946684800,4102444800);
  const fresh=epoch!==null&&issued!==null&&now-epoch*1000<=30*minute&&now-issued*1000<=30*minute&&Math.max(epoch,issued)*1000<=now+minute;
  const mp3=fresh&&typeof state.mp3==='string'&&/^mp3\/atis-efnu-\d{6}Z-\d{10}\.mp3$/.test(state.mp3)?EFNU_SOURCE+state.mp3:null;
  return {source:EFNU_SOURCE,status:!r?'missing':fresh?'observed':'stale',frequencyMhz:'135.825',
    observedAt:epoch===null?null:utc(epoch*1000),issuedAt:issued===null?null:utc(issued*1000),checkedAt:utc(now),
    identifier:typeof r?.repid==='string'&&/^[A-Z]$/.test(r.repid)?r.repid:null,
    windDirection:range(r?.wind_dir,0,360),windKt:range(r?.wind_kt,0,200),gustKt:range(r?.wind_gust_kt,0,250),
    visibilityKm:range(r?.vis_km,0,100),temperatureC:range(r?.temperature,-80,65),dewpointC:range(r?.dewpoint,-100,65),qnhHpa:range(r?.qnh,850,1100),
    clouds:Array.isArray(r?.clouds)?r.clouds.slice(0,8).flatMap(c=>Array.isArray(c)&&['FEW','SCT','BKN','OVC'].includes(c[0])&&range(c[1],0,600)!==null?[{amount:c[0],heightFt:c[1]*100}]:[]):[],
    features:textLines(r?.features,12,30),message:textLines(state?.message,24,220),audioUrl:mp3};
}

export function createAirportSources({fetchImpl=fetch,now=Date.now}={}){
  async function read(url,format,maxBytes){
    const options={redirect:'manual',signal:AbortSignal.timeout(6000),headers:{'User-Agent':'NatGlobeAviation-EFB/2.0'}};
    let r=await fetchImpl(url,options);
    // WordPress periodically adds a cron query to this public directory page.
    // Follow one redirect to that exact same page only; never follow other URLs.
    if(url===DIRECTORY && [301,302,303,307,308].includes(r.status)){
      const location=r.headers.get('location');await r.body?.cancel();
      const next=new URL(location||'',DIRECTORY), original=new URL(DIRECTORY);
      if(next.origin!==original.origin||next.pathname!==original.pathname||next.username||next.password
        ||!/^\?doing_wp_cron=\d{1,15}(?:\.\d{1,30})?$/.test(next.search))throw new Error('Unexpected directory redirect');
      r=await fetchImpl(next.href,options);
    }
    if(!r.ok){await r.body?.cancel();throw new Error('Upstream unavailable');}
    let total=0;const chunks=[];
    for await(const chunk of r.body){total+=chunk.byteLength;if(total>maxBytes)throw new Error('Upstream response too large');chunks.push(chunk);}
    const text=Buffer.concat(chunks,total).toString('utf8');return format==='json'?JSON.parse(text):text;
  }
  function cache(load,ttl,offline){
    let pending,cached,expires=0;
    return async ()=>{
      if(expires>now())return cached;
      pending??=load().then(value=>{cached=value;expires=now()+ttl;}).catch(()=>{cached=offline;expires=now()+30000;}).finally(()=>{pending=null;});
      await pending;return cached;
    };
  }
  const fuel=cache(async()=>parseFuelDirectory(await read(FUEL_FEED,'json',64*1024),now()),5*minute,null);
  const directory=cache(async()=>parseAirfieldDirectory(await read(DIRECTORY,'html',512*1024)),12*60*minute,null);
  const efnu=cache(async()=>parseEfnuInformation(await read(EFNU_FEED,'json',64*1024),now()),45*1000,null);
  return {
    async fuelFor(code){
      if(!/^EF[A-Z]{2}$/.test(code))return {status:'unsupported',prices:[],source:FUEL_SOURCE};
      const feed=await fuel();return feed?.get(code)||{status:feed?'missing':'unavailable',prices:[],source:FUEL_SOURCE,supplier:FUEL_SUPPLIER};
    },
    async directoryFor(code){
      if(!/^EF[A-Z]{2}$/.test(code))return null;
      const data=await directory();return data?{...data.rows.get(code),page:data.links.get(code)||null}:null;
    },
    async efnu(){return (await efnu())||{source:EFNU_SOURCE,status:'unavailable',frequencyMhz:'135.825',checkedAt:utc(now())};}
  };
}

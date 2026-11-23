/* Directory, local reports and fuel prices are reference-only. */
(function(root){
  function node(tag,text,className){const element=document.createElement(tag);if(text!==undefined)element.textContent=text;if(className)element.className=className;return element;}
  function link(label,url){const a=node('a',label);a.href=url;a.target='_blank';a.rel='noopener noreferrer';return a;}
  const utc=value=>value&&Number.isFinite(Date.parse(value))?new Date(value).toISOString().slice(0,16).replace('T',' ')+' UTC':'Time unavailable';
  const number=(value,unit,dp=0)=>Number.isFinite(value)?`${value.toFixed(dp)} ${unit}`:'—';
  const brand='https://info.efnu.fi/';
  function action(label,run){const button=node('button',label);button.type='button';button.addEventListener('click',run);return button;}
  function updateEfnuAges(container){
    for(const panel of container.querySelectorAll('[data-efnu-observed]')){
      const age=Date.now()-Date.parse(panel.dataset.efnuObserved), sinceIssue=Date.now()-Date.parse(panel.dataset.efnuIssued);
      const fresh=panel.dataset.efnuStatus==='observed'&&age>=-60000&&age<=30*60000&&sinceIssue>=-60000&&sinceIssue<=30*60000;
      panel.querySelector('.efnu-time').textContent=`Observed ${utc(panel.dataset.efnuObserved)}${Number.isFinite(age)&&age>=-60000?' · '+Math.max(0,Math.floor(age/60000))+' min old':''}${fresh?'':' · Stale or unverified'}`;
      const player=panel.querySelector('audio');
      if(player&&!fresh){player.pause();player.remove();}
      panel.querySelector('.efnu-report-data').hidden=!fresh;
      panel.querySelector('.efnu-stale').hidden=fresh;
    }
  }
  function efnuCard(data){
    if(!data)return null;
    const card=node('section',undefined,'wx-local-airfield efnu-local');
    const head=node('header');head.append(node('h3','EFNU · Nummela Information / ATIS'),link('Open info.efnu.fi',brand));
    card.append(head,node('p','Automated airfield information · 135.825 MHz · Not an ATC clearance, METAR or TAF.'));
    if(data.status==='unavailable'||data.status==='missing'){
      card.append(node('p',data.status==='missing'?'No EFNU broadcast is published.':'EFNU information feed temporarily unavailable.'));return card;
    }
    card.dataset.efnuObserved=data.observedAt||'';card.dataset.efnuIssued=data.issuedAt||'';card.dataset.efnuStatus=data.status;
    card.append(node('p',undefined,'efnu-time'),node('p','Current EFNU information could not be verified. Check the source or the published broadcast.','efnu-stale'));
    const body=node('div',undefined,'efnu-report-data');
    body.append(node('p',`${data.identifier?'Information '+data.identifier+' · ':''}Issued ${utc(data.issuedAt)}`));
    const wind=Number.isFinite(data.windKt)?`${number(data.windDirection,'°')}, ${number(data.windKt,'kt')}${Number.isFinite(data.gustKt)&&data.gustKt>data.windKt?' · gust '+number(data.gustKt,'kt'):''}`:'—';
    const clouds=data.clouds.map(c=>c.amount+' '+c.heightFt+' ft').join(' · ')||'—';
    const metrics=node('dl',undefined,'airport-metrics');
    for(const [name,value] of [['Wind',wind],['Visibility',number(data.visibilityKm,'km',data.visibilityKm<1?1:0)],['Clouds',clouds],['Temp / dew point',number(data.temperatureC,'°C',1)+' / '+number(data.dewpointC,'°C',1)],['QNH',number(data.qnhHpa,'hPa',1)]]){
      const item=node('div');item.append(node('dt',name),node('dd',value));metrics.append(item);
    }
    body.append(metrics);
    if(data.features.length)body.append(node('p','Reported features: '+data.features.join(' · ')));
    if(data.message.length){const read=node('details');read.append(node('summary','Read published broadcast'),node('pre',data.message.join('\n')));body.append(read);}
    if(data.audioUrl&&/^https:\/\/info\.efnu\.fi\/mp3\/atis-efnu-\d{6}Z-\d{10}\.mp3$/.test(data.audioUrl)){
      const audio=node('audio');audio.controls=true;audio.preload='none';audio.src=data.audioUrl;audio.setAttribute('aria-label','Listen to Nummela Information');
      audio.addEventListener('error',()=>audio.replaceWith(link('Audio unavailable here · open EFNU source',brand)));body.append(audio);
    }
    card.append(body);
    const stage=node('div');stage.append(card);updateEfnuAges(stage);return card;
  }
  async function request(path,signal){
    const deadline=AbortSignal.timeout(12000);
    const response=await fetch(path,{cache:'no-store',signal:signal&&AbortSignal.any?AbortSignal.any([signal,deadline]):deadline});
    const data=await response.json();
    if(!response.ok&&!Array.isArray(data.reports))throw new Error(data.error||'Reference unavailable');
    return data;
  }
  function fuelFor(code,signal){return request('/api/booking-ops/airport-reference?'+new URLSearchParams({icao:code,section:'fuel'}),signal);}
  const fuelStatus={missing:'No published supplier price for this airport. That does not establish fuel availability.',unsupported:'This Finnish supplier feed does not cover the airport. Contact the airport or supplier.',unavailable:'The supplier feed is temporarily unavailable.',closed:'The directory lists the supplier’s pump as closed.',stale:'Published price feed is outdated or its timestamp could not be verified. Do not use for a current quote.'};
  root.PilotAirportData={updateEfnuAges,efnuCard,fuelFor,fuelStatus};

  root.createPilotAirportWorkspace=function({getCatalogue,getFavorites,setFavorites,getFlight,showOnMap,assign,openCalculator}){
    const el=id=>document.getElementById(id), results=el('airportResults'), detail=el('airportFileContent'), find=el('airportFind');
    const prefix='/api/booking-ops/';
    const storage='nga-airport-recent-v1';
    let recent=[];
    try{const saved=JSON.parse(localStorage.getItem(storage));if(Array.isArray(saved))recent=saved.filter(x=>/^[A-Z]{4}$/.test(x)).slice(0,12);}catch{}
    let list='recent',code='',view='info',reference,weather,weatherError='',refError='',refJob,wxJob;
    const catalog=()=>getCatalogue().filter(a=>/^[A-Z]{4}$/.test(a.icao));
    const normalize=x=>String(x||'').normalize('NFD').replace(/\p{Diacritic}/gu,'').toUpperCase();
    const apt=()=>reference?.airport||catalog().find(a=>a.icao===code);
    function renderList(){
      const query=normalize(find.value.trim()), favourites=getFavorites();
      const all=catalog(), byCode=new Map(all.map(a=>[a.icao,a]));
      let found=query?all.filter(a=>[a.icao,a.name,a.city].some(text=>normalize(text).includes(query)))
        :list==='catalogue'?all:(list==='favorites'?favourites:recent).map(icao=>byCode.get(icao)||{icao,name:''});
      if(query)found.sort((a,b)=>Number(b.icao===query)-Number(a.icao===query)||Number(b.icao.startsWith(query))-Number(a.icao.startsWith(query))||a.icao.localeCompare(b.icao));
      results.replaceChildren();
      for(const a of found.slice(0,40)){
        const item=action('',()=>select(a.icao));item.setAttribute('aria-pressed',String(a.icao===code));
        item.append(node('strong',a.icao),node('span',(favourites.includes(a.icao)?'★ ':'')+(a.name||a.city||'Airport')));results.append(item);
      }
      if(query&&/^[A-Z]{4}$/.test(query)&&!found.some(a=>a.icao===query))results.prepend(action('Look up '+query,()=>select(query)));
      el('airportSearchStatus').textContent=found.length>40?'Showing the first 40 · search to narrow the list.':!found.length?(query?'No catalogue matches. Try an exact ICAO.':list==='favorites'?'No favorites yet. Save one from its airport file.':list==='recent'?'No recent airports yet.':'Airport catalogue not available.') : '';
    }
    function title(){
      const a=apt(),favorite=getFavorites().includes(code);
      el('airportFileCode').textContent=code||'AIRPORT REFERENCE';el('airportFileTitle').textContent=a?.name||code||'Select an airport';
      const position=[Number.isFinite(a?.lat)&&Number.isFinite(a?.lon)?`${a.lat.toFixed(5)}°, ${a.lon.toFixed(5)}°`:'',Number.isFinite(a?.elevationFt)?`Elevation ${Math.round(a.elevationFt)} ft`:'' ].filter(Boolean);
      el('airportFilePosition').textContent=position.join(' · ')||(code?'Coordinates or elevation not available.':'Find an airport by ICAO or name.');
      el('airportFavoriteBtn').setAttribute('aria-pressed',String(favorite));el('airportFavoriteBtn').textContent=favorite?'★ Saved':'☆ Favorite';el('airportFavoriteBtn').disabled=!code||!Boolean(a||reference);
      el('airportMapBtn').disabled=!Number.isFinite(a?.lat)||!Number.isFinite(a?.lon);
      el('airportFileActions').hidden=!code;el('airportFileTabs').hidden=!code;
    }
    function references(box){
      const links=node('div',undefined,'airport-actions');
      if(reference?.directory?.page)links.append(link('Airport page / charts',reference.directory.page));
      if(code.startsWith('EF'))links.append(link('Finnish eAIP','https://www.ais.fi/eaip/currently_effective/index.html'),link('NOTAM / briefing','https://sky.fintraffic.fi/'));
      if(links.children.length)box.append(links);
    }
    function render(){
      title(); if(!code)return;
      detail.replaceChildren();
      if(view==='weather'){
        const heading=node('header',undefined,'airport-panel-heading');heading.append(node('h3','Weather & ATIS'),action('Refresh reports',()=>loadWeather(true)));detail.append(heading);
        if(!weather){detail.append(node('p',weatherError||'Retrieving station reports…'));return;}
        for(const local of [root.PilotWx.localAirfield(weather.efhvLocal),efnuCard(weather.efnuLocal)])if(local)detail.append(local);
        detail.append(node('h3','Exact-station reports'));
        for(const r of weather.reports){const report=node('section',undefined,'airport-report');report.append(node('strong',r.product.toUpperCase()+' · '+code),node('pre',r.raw||r.error||'No report published for this station.'),node('small',r.source+' · retrieved '+utc(r.retrievedAt)));detail.append(report);}
        if(!weather.efnuLocal)detail.append(node('p','No integrated online ATIS for this airport. Check the current aerodrome sources.'));
        return;
      }
      if(!reference){detail.append(node('p',refError||'Loading airport reference…'));if(refError)detail.append(action('Retry reference',loadReference));return;}
      if(view==='runways'){
        detail.append(node('h3','Runway reference'));
        const strip=node('div',undefined,'airport-runway-list'),seen=new Set();
        for(const runway of reference.runways||[]){
          const key=[runway.ident,runway.reciprocalIdent].filter(Boolean).sort().join('/');if(seen.has(key))continue;seen.add(key);
          const r=node('article');r.append(node('strong',key),node('p',[Number.isFinite(runway.lengthFt)?Math.round(runway.lengthFt*0.3048)+' m':'',Number.isFinite(runway.widthFt)?Math.round(runway.widthFt*0.3048)+' m wide':'',runway.surface].filter(Boolean).join(' · ')||'Dimensions not available'));strip.append(r);
        }
        detail.append(seen.size?strip:node('p','No runway reference available. Check the current airport chart.'));
        detail.append(node('small','OurAirports catalogue. Physical dimensions are not declared takeoff or landing distances.'));references(detail);return;
      }
      if(view==='fuel'){
        const f=reference.fuel;
        const head=node('header',undefined,'airport-panel-heading');head.append(node('h3','Fuel prices'),action('Refresh directory',loadReference));detail.append(head);
        if(f.status!=='listed')detail.append(node('p',fuelStatus[f.status]||'No verified price available.'));
        for(const price of f.prices){
          const row=node('div',undefined,'airport-fuel-row');row.append(node('strong',price.grade),node('span',`€${price.eurPerL.toFixed(3)} / L`));
          if(f.status==='listed')row.append(action('Use in calculator',()=>openCalculator(code,price)));detail.append(row);
        }
        const sources=node('p');sources.append(link('Lentopaikat.fi fuel directory',f.source));if(f.supplier)sources.append(' · ',link('Kanair supplier price list',f.supplier));detail.append(sources);
        if(f.updatedAt)detail.append(node('small',`Feed updated ${utc(f.updatedAt)}${f.date?' · rates effective from '+f.date:''} · verify availability and current price with the supplier.`));
        detail.append(action('Open fuel calculator',()=>openCalculator(code)));return;
      }
      detail.append(node('h3','Communications & references'));
      const channels=[...(reference.directory?.comMhz||[]).map(f=>['Airfield COM',f]),...(reference.directory?.accMhz||[]).map(f=>['ACC',f]),...(code==='EFNU'?[['Nummela Information / ATIS','135.825']]:[])];
      const metrics=node('dl',undefined,'airport-metrics');
      for(const [label,mhz] of channels){const item=node('div');item.append(node('dt',label),node('dd',mhz+' MHz'));metrics.append(item);}
      detail.append(channels.length?metrics:node('p','No frequency listed in the connected directory. Check the current AIP.'));
      if(reference.directory?.comMhz?.length)detail.append(link('Lentopaikat.fi frequency directory',reference.directory.source));
      if(code==='EFNU')detail.append(link('Nummela operator radio information','https://efnu.fi/radio/'));
      references(detail);
    }
    async function loadReference(){
      refJob?.abort();const job=new AbortController();refJob=job;reference=null;refError='';render();
      try{const data=await request(prefix+'airport-reference?'+new URLSearchParams({icao:code}),job.signal);if(refJob===job)reference=data;}
      catch(error){if(refJob===job)refError=error.message;}
      finally{if(refJob===job){refJob=null;render();}}
    }
    async function loadWeather(refresh=false){
      if(!code||(!refresh&&(weather||wxJob)))return;
      wxJob?.abort();const job=new AbortController();wxJob=job;weather=null;weatherError='';render();
      try{const data=await request(prefix+'weather-report?'+new URLSearchParams({icao:code,product:'summary'}),job.signal);if(wxJob===job)weather=data;}
      catch{if(wxJob===job)weatherError='Station weather unavailable. Retry or check the published source.';}
      finally{if(wxJob===job){wxJob=null;render();}}
    }
    function select(next){
      if(!/^[A-Z]{4}$/.test(next))return;
      if(code===next){renderList();return;}
      wxJob?.abort();wxJob=null;weather=null;weatherError='';code=next;
      recent=[code,...recent.filter(x=>x!==code)].slice(0,12);try{localStorage.setItem(storage,JSON.stringify(recent));}catch{}
      renderList();void loadReference();if(view==='weather')void loadWeather();
    }
    find.addEventListener('input',renderList);el('airportFindForm').addEventListener('submit',event=>{
      event.preventDefault();const input=find.value.trim().toUpperCase();if(/^[A-Z]{4}$/.test(input))select(input);else results.querySelector('button')?.click();
    });
    for(const button of document.querySelectorAll('[data-airport-list]'))button.addEventListener('click',()=>{
      list=button.dataset.airportList;find.value='';for(const b of document.querySelectorAll('[data-airport-list]'))b.setAttribute('aria-pressed',String(b===button));renderList();
    });
    for(const button of document.querySelectorAll('[data-airport-view]'))button.addEventListener('click',()=>{
      view=button.dataset.airportView;for(const b of document.querySelectorAll('[data-airport-view]'))b.setAttribute('aria-pressed',String(b===button));render();if(view==='weather')void loadWeather();
    });
    for(const button of document.querySelectorAll('[data-airport-assign]'))button.addEventListener('click',()=>{if(code)void assign(code,button.dataset.airportAssign);});
    el('airportMapBtn').addEventListener('click',()=>showOnMap(code,apt()));
    el('airportFavoriteBtn').addEventListener('click',()=>{
      const f=getFavorites();if(!f.includes(code)&&f.length>=50){el('airportSearchStatus').textContent='Maximum 50 favorites. Remove one first.';return;}
      setFavorites(f.includes(code)?f.filter(x=>x!==code):[...f,code]);title();renderList();
    });
    return {open(next){if(next)select(next);else if(!code){const flight=getFlight();select(recent[0]||flight?.operationalFlightPlan?.departure||flight?.dep||'EFHV');}renderList();},refreshCatalogue(){if(code)title();renderList();}};
  };
})(globalThis);

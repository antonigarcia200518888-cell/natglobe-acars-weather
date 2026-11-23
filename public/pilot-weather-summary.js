/* Partial TAF decode: keep the raw report alongside this text. DDHH intervals
   are UTC day-of-month/hour, not inferred dates or guarantees of currency. */
(function(root){
  function forecast(raw, now=new Date()){
    if (!raw || raw==='NOT AVAILABLE') return 'No forecast available.';
    const text=String(raw).toUpperCase(), lines=[];
    const issue=text.match(/\b(\d{2})(\d{2})(\d{2})Z\b/);
    if(issue)lines.push('Issued day '+issue[1]+' '+issue[2]+':'+issue[3]+'Z');
    const period=text.match(/\b(\d{2})(\d{2})\/(\d{2})(\d{2})\b/);
    if(period){
      const nearest=(day,hour,anchor)=>[-1,0,1].flatMap(m=>{
        const date=new Date(Date.UTC(anchor.getUTCFullYear(),anchor.getUTCMonth()+m,Number(day)));
        if(date.getUTCDate()!==Number(day) || Number(hour)>24)return [];
        date.setUTCHours(Number(hour));return [date];
      }).sort((a,b)=>Math.abs(a-anchor)-Math.abs(b-anchor))[0];
      const start=nearest(period[1],period[2],now),end=start && nearest(period[3],period[4],start);
      const state=!end || end<=start?'verify window':now<start?'not yet valid':now>=end?'expired':'within published window';
      lines.push('Valid day '+period[1]+' '+period[2]+':00Z to day '+period[3]+' '+period[4]+':00Z ('+state+')');
    }
    // Keep a probability qualifier attached to its temporary conditions.
    for(const group of text.replace(/(PROB(?:30|40))\s+TEMPO\b/g,'$1_TMP').split(/(?=\b(?:TEMPO|BECMG|FM\d{6}|PROB(?:30|40)(?:_TMP)?)\b)/)){
      const type=group.match(/^(TEMPO|BECMG|FM\d{6}|PROB(?:30|40)(?:_TMP)?)\b/);
      const descriptor=type ? ({TEMPO:'Temporary',BECMG:'Becoming',PROB30:'30% probability',PROB40:'40% probability',PROB30_TMP:'30% probability of temporary conditions',PROB40_TMP:'40% probability of temporary conditions'}[type[1]] || 'From day '+type[1].slice(2,4)+' '+type[1].slice(4,6)+':'+type[1].slice(6)+'Z') : 'Initial forecast';
      const components=[];
      if(type){const time=group.match(/\b\d{4}\/\d{4}\b/);if(time)components.push(time[0]+' UTC');}
      const wind=group.match(/\b(VRB|\d{3})(\d{2,3})(?:G(\d{2,3}))?KT\b/);
      if(wind)components.push((wind[1]==='VRB'?'variable':wind[1]+'° true')+' at '+Number(wind[2])+' kt'+(wind[3]?', gust '+wind[3]+' kt':''));
      for(const layer of group.matchAll(/\b(FEW|SCT|BKN|OVC|VV)(\d{3})(?:CB|TCU)?\b/g))components.push(({FEW:'few',SCT:'scattered',BKN:'broken',OVC:'overcast',VV:'vertical visibility'}[layer[1]])+' '+Number(layer[2])*100+' ft');
      if(components.length || type)lines.push(descriptor+': '+(components.join(', ') || 'see raw group'));
    }
    return lines.join(' · ') || 'See raw forecast for conditions and validity.';
  }
  const utc = value => new Date(value).toISOString().slice(0,16).replace('T',' ')+' UTC';
  function updateLocalAges(container) {
    const now=Date.now();
    for(const label of container.querySelectorAll('[data-efhv-measured]')) {
      const measured=Date.parse(label.dataset.efhvMeasured), age=Math.max(0,Math.floor((now-measured)/60000));
      const unverified=measured>now+60000, stale=now-measured>15*60000;
      label.textContent=`Sensor timestamp: ${utc(measured)} · ${unverified?'Check device clock':`${age} min old${stale?' · Stale':''}`}`;
      const chart=label.parentElement.querySelector('.efhv-wind-chart');
      if(chart) chart.hidden=stale||unverified;
      label.parentElement.querySelector('.efhv-wind-unavailable').hidden=Boolean(chart&&!chart.hidden);
    }
  }

  function localAirfield(data) {
    if(!data) return null;
    const node=(tag,text)=>{const el=document.createElement(tag);el.textContent=text;return el;};
    const section=node('section','');section.className='wx-local-airfield';
    const heading=node('h3','EFHV · Hyvinkää local weather');
    const link=node('a','Open efhv.fi');link.href='https://efhv.fi/';link.target='_blank';link.rel='noopener noreferrer';
    const header=node('header','');header.append(heading,link);section.append(header);
    const status=node('p',data.unavailable?'Source temporarily unavailable.':'Sensor timestamp could not be verified.');
    if(data.observedAt)status.dataset.efhvMeasured=data.observedAt;
    section.append(status);
    const unavailable=node('p','No fresh wind graphic verified.');unavailable.className='efhv-wind-unavailable';section.append(unavailable);
    if(data.windChart) {
      const chart=node('details','');chart.className='efhv-wind-chart';
      const img=node('img','');img.loading='lazy';img.alt='Wind graphic published by the independent EFHV weather site';
      img.src='data:image/png;base64,'+data.windChart.png;
      img.addEventListener('error',()=>{chart.remove();unavailable.hidden=false;});
      chart.append(node('summary','View local wind graphic'),img,node('small','Graphic updated '+utc(data.windChart.updatedAt)));section.append(chart);
    }
    if(data.forecast) {
      const f=data.forecast;
      section.append(node('p',`FMI forecast for ${utc(f.validAt)} · ${f.temperatureC} °C · Wind ${String(f.direction).padStart(3,'0')}° / ${f.speedKt} kt${f.gustKt!==null?' (gust '+f.gustKt+' kt)':''}`));
    } else section.append(node('p','No upcoming forecast slot verified.'));
    section.append(node('small','Independent efhv.fi sensor and FMI forecast as published by efhv.fi. Not METAR or TAF. Nearby official reports are listed separately.'));
    updateLocalAges(section);return section;
  }
  root.PilotWx={forecast,localAirfield,updateLocalAges};
})(globalThis);

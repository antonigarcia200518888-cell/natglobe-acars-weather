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
  root.PilotWx={forecast};
})(globalThis);

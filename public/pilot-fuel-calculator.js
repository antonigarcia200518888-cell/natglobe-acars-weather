/* Independent planning calculator; never changes aircraft records or an OFP. */
(function(root){
  const litresPer={l:1,usg:3.785411784,imp:4.54609};
  const kgPer={kg:1,lb:0.45359237,t:1000};
  const labels={l:'L',usg:'US gal',imp:'Imp gal',kg:'kg',lb:'lb',t:'t'};
  function number(value){
    const text=String(value??'').trim();
    return /^-?(?:\d+(?:[.,]\d+)?|[.,]\d+)$/.test(text)?Number(text.replace(',','.')):NaN;
  }
  function calculate({unit,amount,density,price,correction,temp,referenceTemp,coefficient}){
    const value=number(amount), rho=number(density), euro=number(price);
    const isVolume=Object.hasOwn(litresPer,unit), isMass=Object.hasOwn(kgPer,unit);
    if(!Number.isFinite(value)||value<0||value>1000000||(!isVolume&&!isMass))return null;
    let workingDensity=rho>=0.5&&rho<=1.2?rho:null;
    if(correction){
      const actual=number(temp),reference=number(referenceTemp),beta=number(coefficient);
      workingDensity=workingDensity!==null&&Math.abs(actual-reference)<=50&&Math.abs(actual)<=60&&Math.abs(reference)<=60&&beta>0&&beta<=0.003
        ?workingDensity/(1+beta*(actual-reference)):null;
    }
    const litres=isVolume?value*litresPer[unit]:(workingDensity===null?null:value*kgPer[unit]/workingDensity);
    const kg=isMass?value*kgPer[unit]:(workingDensity===null?null:litres*workingDensity);
    return {litres,kg,density:workingDensity,cost:litres!==null&&euro>=0&&euro<=100?litres*euro:null,
      values:{l:litres,usg:litres===null?null:litres/litresPer.usg,imp:litres===null?null:litres/litresPer.imp,kg,lb:kg===null?null:kg/kgPer.lb,t:kg===null?null:kg/1000}};
  }
  root.PilotFuel={calculate};

  root.createPilotFuelCalculator=function({lookupFuel}){
    const el=id=>document.getElementById(id), panel=el('fuelCalculator');
    const airport=el('fuelCalcAirport'), grade=el('fuelCalcGrade'), price=el('fuelCalcPrice'), source=el('fuelCalcSource');
    const status=el('fuelCalcLookupStatus'), lookup=el('fuelCalcLookup'), priceChoice=el('fuelCalcPublished'), apply=el('fuelCalcApply');
    const correction=el('fuelCalcCorrect');
    let records=[], search, applied=false;
    const fmt=new Intl.NumberFormat(undefined,{maximumFractionDigits:3});
    const euro=new Intl.NumberFormat(undefined,{style:'currency',currency:'EUR'});
    function refresh(){
      el('fuelCalcTemperature').hidden=!correction.checked;
      const details=calculate({amount:el('fuelCalcAmount').value,unit:el('fuelCalcUnit').value,density:el('fuelCalcDensity').value,
        price:price.value,correction:correction.checked,temp:el('fuelCalcTemp').value,referenceTemp:el('fuelCalcRefTemp').value,coefficient:el('fuelCalcBeta').value});
      for(const [unit,label] of Object.entries(labels)){
        const value=details?.values[unit];el('fuelCalcOut-'+unit).textContent=Number.isFinite(value)?`${fmt.format(value)} ${label}`:'—';
      }
      el('fuelCalcCost').textContent=Number.isFinite(details?.cost)?euro.format(details.cost):'—';
      el('fuelCalcHint').textContent=!details?'Enter a fuel quantity to convert.'
        :details.density===null?(correction.checked?'Check reference density, temperatures and supplier expansion coefficient.':'Enter the supplier’s fuel density to convert between volume and mass.')
        :`Density used: ${details.density.toFixed(4)} kg/L${correction.checked?' · temperature adjusted estimate':''}.`;
    }
    function clearPublished(){
      search?.abort();search=null;records=[];priceChoice.replaceChildren();priceChoice.disabled=true;apply.disabled=true;lookup.disabled=false;status.textContent='';
      if(applied)price.value='';applied=false;source.textContent='Manual price · EUR per litre';refresh();
    }
    function setAirport(code){airport.value=code;clearPublished();}
    async function load(){
      clearPublished();const code=airport.value.trim().toUpperCase();airport.value=code;
      if(!/^[A-Z]{4}$/.test(code)){status.textContent='Enter a four-letter airport ICAO.';return;}
      const current=new AbortController();search=current;lookup.disabled=true;status.textContent=`Checking ${code} published fuel prices…`;
      try{
        const data=await lookupFuel(code,current.signal);
        if(search!==current)return;
        records=data.status==='listed'?(data.prices||[]):[];
        for(const [i,item] of records.entries()){
          const option=document.createElement('option');option.value=String(i);option.textContent=`${item.grade} · €${item.eurPerL.toFixed(3)}/L${item.date?' · '+item.date:' · date unlisted'}`;
          priceChoice.append(option);
        }
        priceChoice.disabled=apply.disabled=!records.length;
        status.textContent=records.length?'Published prices are estimates. Confirm the grade and current price with the supplier.'
          :((root.PilotAirportData?.fuelStatus[data.status]||'No verified price published for this airport.')+' Enter a supplier quote manually.');
      }catch{if(search===current)status.textContent='Could not load prices. Enter a supplier quote manually.';}
      finally{if(search===current){search=null;lookup.disabled=false;}}
    }
    function usePrice(code,item){
      if(airport.value!==code)setAirport(code);grade.value=item.grade;price.value=String(item.eurPerL);applied=true;
      const link=document.createElement('a');link.href=item.source;link.target='_blank';link.rel='noopener noreferrer';link.textContent='Lentopaikat.fi';
      source.replaceChildren(`${code} · ${item.grade} · `,link,` · ${item.date?'effective from '+item.date:'effective date not listed'} · verify with supplier`);refresh();
    }
    lookup.addEventListener('click',load);airport.addEventListener('input',clearPublished);
    apply.addEventListener('click',()=>{const selected=records[Number(priceChoice.value)];if(selected)usePrice(airport.value,selected);});
    price.addEventListener('input',()=>{applied=false;source.textContent='Manual price · EUR per litre';});
    grade.addEventListener('change',()=>{if(applied){price.value='';applied=false;source.textContent='Manual price · EUR per litre';}refresh();});
    panel.addEventListener('input',refresh);panel.addEventListener('change',refresh);clearPublished();
    return {setAirport,usePrice,load};
  };
})(globalThis);

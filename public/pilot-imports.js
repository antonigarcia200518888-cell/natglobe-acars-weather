/* Review-first file interchange. No authenticated ForeFlight connection. */
window.createPilotImports=function({context,ensureSaved,openView,onTrack}){
  const dialog=document.createElement('dialog');dialog.className='import-dialog';dialog.setAttribute('aria-labelledby','importTitle');
  dialog.innerHTML=`<header><h2 id="importTitle">Import flight plan</h2><button type="button" data-action="close">Close</button></header>
    <p id="importExplanation"></p><section data-stage="source"><label>Choose a file (8 MiB maximum)<input id="importFile" type="file"></label>
    <label id="routePasteLabel">Or paste route / ICAO FPL text<textarea id="importText" rows="3" placeholder="Departure: EFHK&#10;Destination: EETN&#10;Route: EFHK DCT EETN"></textarea></label>
    <button type="button" data-action="review">Review file</button></section>
    <section data-stage="review" hidden><div id="importWarnings" class="import-alert"></div><p id="importMatches"></p>
    <div class="import-grid"><label>Save to flight<select id="importTarget"></select></label><label>Flight category<select id="importCategory"><option value="operational">Operational</option><option value="private">Private</option><option value="training">Training</option></select></label></div>
    <p id="importFieldHint">Checked fields will be applied. Existing populated fields are protected until you select them.</p><div id="importFields" class="import-review"></div>
    <p id="importTrackSummary"></p><button type="button" data-action="preview" hidden>Preview recorded track on map</button>
    <label id="duplicateLabel" hidden><input id="importDuplicate" type="checkbox"> I want another explicit association for this already-saved source file.</label>
    <div class="import-actions"><button type="button" data-action="back">Choose another file</button><button type="button" class="primary" data-action="confirm">Confirm &amp; open OFP</button></div></section>
    <section data-stage="files" hidden><div id="importSavedFiles"></div></section><p id="importStatus" role="status" aria-live="polite"></p>`;
  document.body.append(dialog);
  const el=id=>dialog.querySelector('#'+id),action=name=>dialog.querySelector('[data-action="'+name+'"]');
  let kind='plan',bytes=null,name='',review=null,busy=false,abort=null;
const labels={departure:'Departure ICAO',destination:'Destination ICAO',alternate:'Alternate ICAO',route:'Planned route',flightRules:'Flight rules',cruiseFlightLevel:'Cruise level',aircraftRegistration:'Registration',aircraftCode:'Aircraft ICAO type',callsign:'Aircraft identification / callsign',estimatedEnrouteMinutes:'Planned EET (minutes)',plannedDepartureUtc:'Planned departure (UTC)'};
  function status(s){el('importStatus').textContent=s;}
  function stage(name){dialog.querySelectorAll('[data-stage]').forEach(s=>s.hidden=s.dataset.stage!==name);}
  function lock(value){busy=value;dialog.querySelectorAll('button,input,select,textarea').forEach(n=>n.disabled=value);if(!value&&review&&!review.storageAvailable)action('confirm').disabled=true;}
  function close(){if(busy)return;dialog.close();bytes=null;review=null;}
  function selected(){return context.all().find(f=>f.id===el('importTarget').value);}
  async function request(path,options={}){
    const response=await fetch('/api/booking-ops/imports'+path,{cache:'no-store',...options});
    const data=await response.json().catch(()=>({}));if(!response.ok)throw Error(data.error||'Import unavailable ('+response.status+').');return data;
  }
  function fields(){
    const current=selected(),old=current?.operationalFlightPlan||{},parsed=review.parsed;
    el('importCategory').value=current?.management?.category||'operational';el('importFields').replaceChildren();
    el('importFieldHint').hidden=kind==='track';
    for(const [key,label] of Object.entries(labels)){
      if(kind==='track'&&(current||!['departure','destination','aircraftRegistration','flightRules','plannedDepartureUtc'].includes(key)))continue;
      const row=document.createElement('div');row.className='import-field';
      const check=document.createElement('input');check.type='checkbox';check.dataset.importField=key;check.setAttribute('aria-label','Apply '+label);
      const previous=key==='plannedDepartureUtc'?(old.scheduledOutIso|| (old.dateUtc&&old.scheduledOutUtc?old.dateUtc+'T'+old.scheduledOutUtc+'Z':'')):old[key];
      const value=parsed.fields?.[key];check.checked=!current||!previous;
      const caption=document.createElement('span');caption.textContent=label;const detail=document.createElement('small');
      detail.textContent=current?'Existing: '+(previous||'Not set'):(value!=null&&value!==''?'Imported':'Missing — pilot input');caption.append(detail);
      const input=document.createElement(key==='flightRules'?'select':'input');input.dataset.importValue=key;
      if(key==='flightRules')for(const v of ['','IFR','VFR'])input.add(new Option(v||'Choose rules',v));
      else input.type=key==='estimatedEnrouteMinutes'?'number':key==='plannedDepartureUtc'?'datetime-local':'text';
      input.setAttribute('aria-label',label);input.value=key==='plannedDepartureUtc'?String(value||'').slice(0,16):value??'';
      if(key==='estimatedEnrouteMinutes'){input.min=0;input.max=1440;input.step=1;}
      if(key==='route')input.maxLength=240;
      if(['departure','destination','alternate'].includes(key)){input.maxLength=4;input.autocapitalize='characters';}
      input.addEventListener('input',()=>{check.checked=true;detail.textContent='Pilot correction — replaces checked field';});
      row.append(check,caption,input);el('importFields').append(row);
    }
  }
  async function open(type='plan'){
    if(busy||!(await ensureSaved()))return;
    kind=type;review=null;bytes=null;stage('source');status('');
    el('importTitle').textContent=kind==='track'?'Import track log':'Import from ForeFlight / route file';
    el('importExplanation').textContent=kind==='track'?'ForeFlight Mobile: KML. ForeFlight Web: KML, GPX or CSV. This creates a recorded track, not a verified logbook entry.':'Paste shared route text or ICAO FPL. Compatible Garmin FPL and GPX route files are also accepted; GPX route export from ForeFlight is not assumed. No account connection or flight filing.';
    el('importFile').accept=kind==='track'?'.kml,.gpx,.csv':'.txt,.fpl,.gpx';el('importFile').value='';el('importText').value='';el('routePasteLabel').hidden=kind==='track';
    if(!dialog.open)dialog.showModal();action('close').focus();
  }
  action('review').onclick=async()=>{
    lock(true);status('Validating source…');
    try{
      const file=el('importFile').files[0];if(file&&file.size>8388608)throw Error('Maximum file size is 8 MiB.');
      name=file?.name||'route.txt';bytes=file?await file.arrayBuffer():new TextEncoder().encode(el('importText').value);
      if(!bytes.byteLength)throw Error('Choose a file or paste a route first.');
      review=await request('/preview?kind='+kind+'&name='+encodeURIComponent(name),{method:'POST',headers:{'Content-Type':'application/octet-stream'},body:bytes});
      el('importWarnings').replaceChildren();for(const warning of review.parsed.warnings){const p=document.createElement('p');p.textContent=warning;el('importWarnings').append(p);}
      el('importMatches').textContent=review.matches.length?'Possible matches (not selected automatically): '+review.matches.map(m=>m.id+' · '+m.reasons.join(', ')).join(' / '):'No matching flight found. You can choose an existing flight or create a new one.';
      el('importTarget').replaceChildren(new Option('Create a new flight',''));
      for(const f of context.all())el('importTarget').add(new Option(f.id+' · '+(f.operationalFlightPlan?.departure||f.dep)+' → '+(f.operationalFlightPlan?.destination||f.arr),f.id));
      el('duplicateLabel').hidden=!review.duplicates.length;el('importDuplicate').checked=false;
      fields();const summary=review.parsed.summary;
      el('importTrackSummary').textContent=summary?summary.pointCount+' recorded points · '+summary.distanceNm+' NM sampled distance · '+(summary.durationSeconds?Math.round(summary.durationSeconds/60)+' min elapsed':'Timing unavailable')+' · '+review.parsed.altitudeReference:'';
      action('preview').hidden=kind!=='track';action('confirm').textContent=kind==='track'?'Save recorded track':'Confirm & open OFP';stage('review');
      status(review.storageAvailable?'Review all checked fields before confirming.':'Storage is offline. You can review or preview, but imports cannot be saved.');
    }catch(e){status(e.message);}finally{lock(false);}
  };
  el('importTarget').onchange=fields;
  action('confirm').onclick=async()=>{
    if(!review?.storageAvailable){status('Persistent storage is offline. Nothing has been saved.');return;}
    lock(true);status('Saving reviewed import…');
    try{
      const corrections={},approved=[];
      for(const input of dialog.querySelectorAll('[data-import-value]')){
        const key=input.dataset.importValue;let value=input.value.trim();
        if(key==='plannedDepartureUtc')value=value?value.slice(0,16)+'Z':'';else if(key==='estimatedEnrouteMinutes')value=value===''?'':Number(value);else value=value.toUpperCase();
        corrections[key]=value;if(dialog.querySelector('[data-import-field="'+key+'"]').checked)approved.push(key);
      }
      const target=el('importTarget').value;
      const options={target,revision:review.revisions[target],category:el('importCategory').value,sha256:review.parsed.sha256,fields:approved,corrections,allowDuplicate:el('importDuplicate').checked};
      const result=await request('/confirm?kind='+kind+'&name='+encodeURIComponent(name),{method:'POST',headers:{'Content-Type':'application/octet-stream','X-NGA-Import':JSON.stringify(options)},body:bytes});
      context.upsert(result.request);dialog.close();await context.activate(result.request,kind==='plan'?'flight-plan':'dashboard');
      if(kind==='track')onTrack(review.parsed,true);bytes=null;review=null;
    }catch(e){status(e.message);}finally{lock(false);}
  };
  action('preview').onclick=()=>{const track=review.parsed;dialog.close();onTrack(track);};
  // A map preview never writes a record. Reopening the source is deliberate.
  action('back').onclick=()=>{stage('source');review=null;status('');};action('close').onclick=close;
  dialog.addEventListener('cancel',e=>{if(busy)e.preventDefault();});
  async function files(){
    const file=context.current();if(!file)return open('track');
    stage('files');el('importTitle').textContent='Imported sources · '+file.id;el('importExplanation').textContent='Original files are private to the authorized pilot workspace. Recorded tracks never replace the planned route.';status('Loading saved sources…');dialog.showModal();
    try{
      const data=await request('/files/'+encodeURIComponent(file.id));el('importSavedFiles').replaceChildren();
      for(const f of data.imports){
        const row=document.createElement('div');row.className='import-actions';const title=document.createElement('p');title.textContent=f.filename+' · '+f.kind+' · '+new Date(f.created_at).toISOString().slice(0,16)+'Z';
        const link=document.createElement('a');link.href='/api/booking-ops/imports/'+f.id+'?download=1';link.download=f.filename;link.textContent='Download original';row.append(title,link);
if(f.kind==='track'){const b=document.createElement('button');b.type='button';b.textContent=f.summary?.replayAvailable?'View / replay':'View track';b.onclick=async()=>{b.disabled=true;try{const data=await request('/'+f.id);dialog.close();onTrack(data.parsed,true);}catch(e){status(e.message);}finally{b.disabled=false;}};row.append(b);}
        el('importSavedFiles').append(row);
      }
      status(data.imports.length?'':'No imported sources saved for this flight.');
    }catch(e){status(e.message);}
  }
  return {open,files};
};

/* Pilot-only metadata and attachments. No booking, payment or release side effects. */
window.PilotFileTools = { create({onSave}) {
  const by=id=>document.getElementById(id);
  const api=id=>'/api/booking-ops/requests/'+encodeURIComponent(id);
  let current=null, fingerprint=null, listing=0;
  async function json(path,options) {
    const r=await fetch(path,{cache:'no-store',...options});
    const data=await r.json().catch(()=>({}));
    if (!r.ok) throw new Error(data.error || 'Request failed ('+r.status+').');
    return data;
  }
  const training=()=>{by('trainingFields').hidden=by('fileKind').value!=='training';};
  const link=(text,href)=>Object.assign(document.createElement('a'),{textContent:text,href});
  async function list(){
    const id=current?.id, generation=++listing;
    const container=by('attachmentList'),status=by('attachmentStatus');
    container.replaceChildren();
    if (!id || !by('flightManagement').open) return;
    status.textContent='Loading documents…';
    try {
      const base=api(id)+'/documents';
      const {documents}=await json(base);
      if(generation!==listing) return;
      status.textContent=documents.length ? documents.length+' of 10 attachments' : 'No documents attached.';
      for(const doc of documents){
        const path=base+'/'+encodeURIComponent(doc.id), row=document.createElement('div');
        row.className='flight-kind-actions';
        const name=document.createElement('span');name.textContent=doc.name;
        const view=Object.assign(document.createElement('button'),{type:'button',textContent:'View'});
        view.onclick=()=>{
          const d=document.createElement('dialog'), close=Object.assign(document.createElement('button'),{type:'button',textContent:'Back to flight'}),frame=document.createElement('iframe');
          d.className='attachment-reader';d.setAttribute('aria-label',doc.name);frame.title=doc.name;frame.src=path+'?view=1';close.onclick=()=>d.close();
          d.append(close,frame);document.body.append(d);d.addEventListener('close',()=>d.remove(),{once:true});d.showModal();close.focus();
        };
        const remove=Object.assign(document.createElement('button'),{type:'button',textContent:'Remove'});
        remove.onclick=async()=>{
          if(current?.id!==id || !confirm('Permanently remove “'+doc.name+'” from this flight?')) return;
          remove.disabled=true;
          try {await json(path,{method:'DELETE'});if(current?.id===id) await list();}
          catch(e){if(current?.id===id)status.textContent=e.message;remove.disabled=false;}
        };
        row.append(name,view,link('Download',path),remove);container.append(row);
      }
    }catch(e){if(generation===listing) status.textContent=e.message;}
  }
  by('fileKind').onchange=training;
  by('flightManagement').addEventListener('toggle',list);
  by('fileKindSave').onclick=async()=>{
    const id=current?.id;if(!id) return;
    by('fileKindSave').disabled=true;by('fileKindStatus').textContent='Saving…';
    try {
      const {management}=await json(api(id)+'/management',{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({category:by('fileKind').value,instructor:by('fileInstructor').value,exercises:by('fileExercises').value,progress:by('fileProgress').value})});
      onSave(id,management);
      if(current?.id===id)by('fileKindStatus').textContent='Saved to flight file.';
    } catch(e){if(current?.id===id)by('fileKindStatus').textContent=e.message;}
    finally{by('fileKindSave').disabled=false;}
  };
  by('uploadAttachment').onclick=async()=>{
    const id=current?.id, file=by('fileAttachment').files[0], status=by('attachmentStatus');
    if(!id || !file || file.size>2097152 || !['image/png','application/pdf','image/jpeg'].includes(file.type)){status.textContent='Select a PDF, JPEG or PNG no larger than 2 MB.';return;}
    by('uploadAttachment').disabled=true;status.textContent='Uploading…';
    try{await json(api(id)+'/documents?name='+encodeURIComponent(file.name),{method:'POST',headers:{'Content-Type':file.type},body:file});if(current?.id===id){by('fileAttachment').value='';await list();}}
    catch(e){if(current?.id===id)status.textContent=e.message;}
    finally{by('uploadAttachment').disabled=false;}
  };
  return {select(req){
    const changed=current?.id!==req?.id;current=req;
    if(changed){by('flightManagement').open=false;by('fileAttachment').value='';by('attachmentList').replaceChildren();by('attachmentStatus').textContent='';listing++;}
    const plan=req?.operationalFlightPlan;
    by('trainingActuals').textContent='Recorded OFF: '+(plan?.actualOffIso || plan?.actualOff || '—')+' · ON: '+(plan?.actualOnIso || plan?.actualOn || '—')+' · See flight log for actuals.';
    const key=JSON.stringify([req?.id,req?.management]);if(key===fingerprint)return;fingerprint=key;
    const m=req?.management || {};
    by('fileKind').value=m.category || '';by('fileKindStatus').textContent='';
    for(const [key,id] of [['instructor','fileInstructor'],['exercises','fileExercises'],['progress','fileProgress']]) by(id).value=m.training?.[key] || '';
    const statement=by('privateStatement');statement.hidden=m.category!=='private';statement.target='_blank';statement.rel='noopener';
    if(req) statement.href='/booking-ops/requests/'+encodeURIComponent(req.id)+'/reimbursement-statement';training();
  }};
}};

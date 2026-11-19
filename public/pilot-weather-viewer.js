/* An in-place reader for the dispatch endpoint's typeset pages. No PDF plug-in,
   new window, or cached weather document is required. Closing aborts the request. */
window.createWeatherReader = function createWeatherReader(dialog) {
  const byId = id => dialog.querySelector('#'+id);
  const back=byId('wxReaderBack'), title=byId('wxReaderTitle'), status=byId('wxReaderStatus');
  const paper=byId('wxReaderPages'), scroller=byId('wxReaderScroll'), mode=byId('wxReaderMode');
  const download=byId('wxReaderDownload'), share=byId('wxReaderShare'), retry=byId('wxReaderRetry');
  let controller, packet, url, file, params, opener, reading, deadline;
  const svgNS='http://www.w3.org/2000/svg';
  function render() {
    const fragment=document.createDocumentFragment();
    packet.pages.forEach((lines,i)=>{
      const sheet=document.createElement('section');
      sheet.className=reading?'wx-reading-page':'wx-paper-page'; sheet.setAttribute('aria-label','Page '+(i+1));
      if (reading) {
        const pre=document.createElement('pre'); pre.textContent=lines.map(l=>l.text).join('\n'); sheet.append(pre);
      } else {
        const svg=document.createElementNS(svgNS,'svg');
        svg.setAttribute('viewBox',`0 0 ${packet.pageWidth} ${packet.pageHeight}`);
        svg.setAttribute('role','img'); svg.setAttribute('aria-label',lines.map(l=>l.text).join('\n'));
        for (const line of lines) {
          const text=document.createElementNS(svgNS,'text');
          text.setAttribute('x',line.x); text.setAttribute('y',line.y); text.setAttribute('font-size',line.size);
          text.setAttribute('font-weight',line.bold?'bold':'normal'); text.setAttribute('fill',`rgb(${line.ink.join(',')})`);
          text.textContent=line.text; svg.append(text);
        }
        sheet.append(svg);
      }
      fragment.append(sheet);
    });
    paper.replaceChildren(fragment);
    mode.setAttribute('aria-pressed',String(reading));
    scroller.scrollTop=0;
  }
  function reset() {
    clearTimeout(deadline);
    controller?.abort(); controller=null; packet=null; file=null;
    download.removeAttribute('href'); if(url) URL.revokeObjectURL(url); url=null;
    paper.replaceChildren(); download.hidden=share.hidden=retry.hidden=mode.hidden=true;
  }
  async function load() {
    reset(); const current=new AbortController(); controller=current;
    const timeout=setTimeout(()=>current.abort(),25000);deadline=timeout;
    status.textContent='Preparing weather document…'; paper.setAttribute('aria-busy','true');
    try {
      const response=await fetch('/api/dispatch-pdf?'+params,{signal:current.signal,cache:'no-store',headers:{Accept:'application/json'}});
      if(!response.ok) throw Error();
      const result=await response.json();
      if(controller!==current) return;
      if(!Array.isArray(result.pages)||!result.pages.length||!result.pdf) throw Error();
      const bytes=Uint8Array.from(atob(result.pdf),c=>c.charCodeAt(0));
      const name='NatGlobe-WX-'+params.get('dep')+'-'+params.get('arr')+'.pdf';
      file=new File([bytes],name,{type:'application/pdf'}); url=URL.createObjectURL(file);
      packet=result; reading=false; render();
      download.href=url; download.download=name; download.hidden=mode.hidden=false;
      share.hidden=!navigator.canShare?.({files:[file]});
      status.textContent=result.pages.length+' '+(result.pages.length===1?'page':'pages')+' · Generated briefing';
    } catch {
      if(controller!==current) return;
      status.textContent=current.signal.aborted?'Weather document timed out. Retry or return to Weather.':'Weather document unavailable. Check your connection and try again.'; retry.hidden=false;
    } finally { clearTimeout(timeout);if(controller===current) paper.removeAttribute('aria-busy'); }
  }
  back.addEventListener('click',()=>dialog.close());
  dialog.addEventListener('close',()=>{ reset(); opener?.focus({preventScroll:true}); });
  mode.addEventListener('click',()=>{reading=!reading;render();});
  retry.addEventListener('click',load);
  share.addEventListener('click',async()=>{ const current=file; if (!current) return;
    try { await navigator.share({files:[current]}); }
    catch(error){if(error.name!=='AbortError' && file===current) status.textContent='Sharing unavailable. Use Save PDF instead.';}
  });
  return { open(query,button) {
    opener=button; params=new URLSearchParams(query); params.set('preview','1');
    title.textContent=params.get('dep')+' → '+params.get('arr')+(params.has('altn')?' · ALT '+params.get('altn'):'');
    if (!dialog.open) dialog.showModal();
    back.focus(); load();
  }};
};

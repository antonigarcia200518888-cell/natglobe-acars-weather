/* Actual recorded positions only. Interpolation is disabled across segment breaks. */
(()=>{
  function prepare(track){
    const points=track.points.map(p=>({...p,ms:Date.parse(p.time)})),features=[];let line=[];
    const flush=()=>{if(line.length>1)features.push({type:'Feature',properties:{recorded:true},geometry:{type:'LineString',coordinates:line}});line=[];};
    for(let i=0;i<points.length;i++){
      const p=points[i],prev=points[i-1];
      if(prev&&(p.segment!==prev.segment||p.ms-prev.ms>120000))flush();
      let lon=p.lon;if(line.length){const last=line.at(-1)[0];while(lon-last>180)lon-=360;while(lon-last< -180)lon+=360;}
      line.push([lon,p.lat]);
    }flush();return {points,geometry:{type:'FeatureCollection',features}};
  }
  function sample(points,ms){
    let lo=0,hi=points.length-1;while(lo<hi){const mid=Math.ceil((lo+hi)/2);if(points[mid].ms<=ms)lo=mid;else hi=mid-1;}
    const a=points[lo],b=points[lo+1];if(!b||ms<=a.ms)return {...a,gap:false};
    if(b.segment!==a.segment||b.ms-a.ms>120000)return {...a,gap:true,next:b.ms};
    const t=Math.max(0,Math.min(1,(ms-a.ms)/(b.ms-a.ms))),lerp=(a,b)=>a==null||b==null?null:a+(b-a)*t;
    let lon=b.lon;while(lon-a.lon>180)lon-=360;while(lon-a.lon< -180)lon+=360;
    return {...a,lat:lerp(a.lat,b.lat),lon:((lerp(a.lon,lon)+540)%360)-180,altitudeFt:lerp(a.altitudeFt,b.altitudeFt),speedKt:lerp(a.speedKt,b.speedKt),ms,gap:false};
  }
  window.PilotTrackMath={prepare,sample};
  window.createPilotReplay=function({map,container,isVisible,context,onDraft}){
    const bar=document.createElement('section');bar.className='replay-bar';bar.hidden=true;bar.setAttribute('aria-label','Recorded flight replay');
    bar.innerHTML='<button type="button" data-r="play">Play</button><input data-r="time" type="range" min="0" max="1000" value="0" aria-label="Recorded flight timeline"><button type="button" data-r="close">Close track</button><div class="replay-data"><strong data-r="readout"></strong><span>Interpolated between recorded samples · not live telemetry</span></div><div class="import-actions"><button type="button" data-r="speed">1× speed</button><button type="button" data-r="camera">Free camera</button><button type="button" data-r="gap" hidden>Skip recording gap</button><button type="button" data-r="fit">Fit track</button></div><p data-r="note" class="replay-data"></p>';
    const profiles=document.createElement('details');profiles.className='replay-profiles';profiles.innerHTML='<summary>Recorded altitude & speed profiles</summary><div data-r="profiles"></div><button type="button" data-r="draft" hidden>Prepare logbook draft</button>';bar.append(profiles);
    container.append(bar);const el=k=>bar.querySelector('[data-r="'+k+'"]');
    let points=[],record=null,start=0,end=0,at=0,playing=false,speed=1,camera='free',frame=0,last=0,lastText=0,pendingGap=null,loadedFlight=null;
    const motion=matchMedia('(prefers-reduced-motion: reduce)');
    function pause(){playing=false;cancelAnimationFrame(frame);frame=0;last=0;el('play').textContent='Play';}
    function show(force=false){
      if(!points.length)return;
      const p=record.summary.replayAvailable?sample(points,at):points[0];
      if(p.gap){pendingGap=p.next;pause();}else pendingGap=null;
      el('gap').hidden=pendingGap==null;map.setReplayPosition(p,camera);
      if(force||performance.now()-lastText>250){
        lastText=performance.now();el('time').value=end>start?Math.round((at-start)/(end-start)*1000):0;
el('readout').textContent=[Number.isFinite(p.ms)?new Date(p.ms).toISOString().slice(11,19)+' Z':'No timestamps',p.altitudeFt==null?'ALT not recorded':Math.round(p.altitudeFt)+' ft recorded',p.speedKt==null?'GS not recorded':Math.round(p.speedKt)+' kt'].join(' · ');
      }
    }
    function tick(time){
      frame=0;if(!playing||document.hidden||!isVisible()){pause();return;}
      if(last)at=Math.min(end,at+Math.min(time-last,100)*speed);last=time;show();
      if(at>=end){pause();show(true);}else if(playing)frame=requestAnimationFrame(tick);
    }
    el('play').onclick=()=>{
      if(playing)return pause();if(!record?.summary.replayAvailable||pendingGap)return;
      if(at>=end)at=start;playing=true;last=0;el('play').textContent='Pause';frame=requestAnimationFrame(tick);
    };
    el('time').oninput=()=>{pause();at=start+(end-start)*Number(el('time').value)/1000;show(true);};
    el('gap').onclick=()=>{at=pendingGap;show(true);};
    el('speed').onclick=()=>{const values=[1,5,10,30];speed=values[(values.indexOf(speed)+1)%values.length];el('speed').textContent=speed+'× speed';};
    el('camera').onclick=()=>{camera=camera==='free'?'top':camera==='top'?'perspective':'free';el('camera').textContent={free:'Free camera',top:'Follow · top down',perspective:'Follow · perspective'}[camera];show(true);};
    el('fit').onclick=()=>map.fitTrack();
    el('draft').onclick=()=>{pause();if(record)onDraft?.(record);};
    function plot(key,label){
      const available=points.filter(p=>Number.isFinite(p[key]));if(!available.length)return '<p>'+label+': not recorded</p>';
      const min=available.reduce((a,p)=>Math.min(a,p[key]),Infinity),max=available.reduce((a,p)=>Math.max(a,p[key]),-Infinity),step=Math.max(1,Math.ceil(points.length/600));
      let path='',connected=false;
      for(let i=0;i<points.length;i++){
        const p=points[i],previous=points[i-1];
        if(p[key]==null||previous&&(p.segment!==previous.segment||p.ms-previous.ms>120000))connected=false;
        if(p[key]==null||i%step&&i!==points.length-1)continue;
        const x=6+588*i/Math.max(1,points.length-1),y=96-80*(p[key]-min)/Math.max(1,max-min);
        path+=(connected?'L':'M')+x.toFixed(1)+' '+y.toFixed(1)+' ';connected=true;
      }
      return '<p>'+label+' · '+Math.round(min)+'–'+Math.round(max)+' · sampled profile, point order</p><svg class="track-profile" viewBox="0 0 600 110" role="img" aria-label="'+label+' profile"><path d="'+path+'" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>';
    }
    function close(){pause();bar.hidden=true;record=null;points=[];map.clearReplay();}
    el('close').onclick=close;document.addEventListener('visibilitychange',()=>{if(document.hidden)pause();});window.addEventListener('hashchange',()=>{if(!isVisible())pause();});
    context.subscribe(view=>{if(record&&view?.id!==loadedFlight)close();});
    let pendingReady=false;
    return {pause,close,onReady(){if(pendingReady&&record){pendingReady=false;map.setRotating(false);map.setMode('globe');map.fitTrack();show(true);}},load(track,saved=false){
      pause();record=track;pendingReady=!map.ready;loadedFlight=context.current()?.id;const prepared=prepare(track);points=prepared.points;
      start=points[0].ms;end=points.at(-1).ms;at=start;map.setMode('globe');map.setTrack(prepared.geometry);bar.hidden=false;
      el('profiles').innerHTML=plot('altitudeFt','Recorded altitude · feet')+plot('speedKt','Recorded ground speed · knots');profiles.open=false;el('draft').hidden=!saved||!context.current();
      el('play').disabled=!track.summary.replayAvailable;el('time').disabled=!track.summary.replayAvailable;
      el('note').textContent=track.summary.pointCount+' samples · '+track.summary.distanceNm+' NM sampled distance · '+track.altitudeReference+(motion.matches?' · Reduced motion: playback is manual.':'')+(track.summary.replayAvailable?'':' · No valid timing: static track only.');
      requestAnimationFrame(()=>{map.fitTrack();show(true);});
    }};
  };
})();

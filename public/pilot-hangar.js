// Do not load Three.js, allocate a WebGL context or start a render loop at launch.
(()=>{
  let instance=null,generation=0;
  const dialog=document.createElement('dialog');dialog.className='airframe-dialog';dialog.setAttribute('aria-labelledby','airframeTitle');
  dialog.innerHTML='<header><div><small>NATGLOBE / HANGAR</small><h2 id="airframeTitle">Airframe study</h2></div><button type="button" data-dismiss>Close</button></header><div class="airframe-viewport"></div><p role="status"></p><footer class="hangar-tools"><button type="button" data-turn="-1">Rotate left</button><button type="button" data-turn="0">Reset view</button><button type="button" data-turn="1">Rotate right</button></footer><small>Representative low-wing single. Illustrative geometry, not a type-specific maintenance reference.</small>';
  document.body.append(dialog);
  const host=dialog.querySelector('.airframe-viewport'),status=dialog.querySelector('[role=status]');
  function controls(on){dialog.querySelectorAll('[data-turn]').forEach(b=>b.disabled=!on);}
  function fallback(){instance?.dispose();instance=null;controls(false);const img=new Image();img.src='/piper-render-transparent.png';img.alt='Side profile of the fleet aircraft';host.replaceChildren(img);status.textContent='3D unavailable on this device or connection. Showing the fleet image.';}
  document.addEventListener('click',async event=>{
    if(!event.target.closest('[data-open-hangar]') || dialog.open)return;
    const ticket=++generation;dialog.showModal();controls(false);status.textContent='Loading interactive model…';
    try{const {mountAirframe}=await import('/pilot-airframe.js?v=2026-11-17-1');if(ticket!==generation || !dialog.open)return;
      instance=mountAirframe(host,fallback);controls(true);status.textContent='Drag to orbit, or use the rotation controls.';
    }catch{if(ticket===generation)fallback();}
  });
  dialog.querySelector('[data-dismiss]').onclick=()=>dialog.close();
  dialog.querySelectorAll('[data-turn]').forEach(b=>b.onclick=()=>instance?.turn(Number(b.dataset.turn)));
  dialog.addEventListener('close',()=>{generation++;instance?.dispose();instance=null;host.replaceChildren();});
})();

/* Favorites and camera settings are device preferences, never flight records. */
window.createPilotAirfields=function({map,getAirports,addAirport}){
  const d=document.createElement('dialog');d.className='airfield-dialog';d.setAttribute('aria-labelledby','airfieldsTitle');
  d.innerHTML='<header><h2 id="airfieldsTitle">Airfields & globe</h2><button type="button" data-a="close">Close</button></header><label>Search available airports<input data-a="search" type="search" placeholder="ICAO or airport name"></label><p>Choose up to 50 favorites for the startup globe. No airports are selected for you.</p><div data-a="list" class="airfield-list"></div><section data-a="detail" hidden><h3 data-a="name"></h3><p data-a="position"></p><div class="import-actions"><button type="button" data-a="favorite">Add favorite</button><button type="button" data-a="view">View on map</button><button type="button" data-a="departure">Use as departure</button><button type="button" data-a="destination">Use as destination</button></div></section><hr><div class="import-actions"><button type="button" data-a="startup">Save current view as startup region</button><label>Terrain exaggeration<select data-a="terrain"><option value="0">Off</option><option value="1">1× · actual elevation</option><option value="1.5">1.5× · exaggerated</option></select></label></div><p>Elevation is a geographic overview, not certified obstacle clearance or synthetic vision. Online tiles may be incomplete.</p><p data-a="status" role="status"></p>';
  document.body.append(d);const el=k=>d.querySelector('[data-a="'+k+'"]');let selected=null;
  try{el('terrain').value=localStorage.getItem('nga-map-terrain')??'1';}catch{}
  function detail(code){
    selected=getAirports().find(a=>a.icao===code);el('detail').hidden=!selected;if(!selected)return;
    el('name').textContent=selected.icao+' · '+selected.name;
    el('position').textContent=Number(selected.lat).toFixed(5)+'°, '+Number(selected.lon).toFixed(5)+'° · Verify airport suitability in current AIP.';
    el('favorite').textContent=map.getFavorites().includes(code)?'Remove favorite':'Add favorite';
  }
  function render(){
    const q=el('search').value.trim().toUpperCase(),fav=map.getFavorites();
    const list=getAirports().filter(a=>a.lat!=null&&a.lon!=null&&/^[A-Z]{4}$/.test(a.icao)&&(!q||[a.icao,a.name].join(' ').toUpperCase().includes(q))).sort((a,b)=>Number(fav.includes(b.icao))-Number(fav.includes(a.icao))||a.icao.localeCompare(b.icao)).slice(0,30);
    el('list').replaceChildren();for(const a of list){const b=document.createElement('button');b.type='button';b.textContent=(fav.includes(a.icao)?'★ ':'')+a.icao+' · '+a.name;b.onclick=()=>detail(a.icao);el('list').append(b);}
    if(!list.length)el('list').textContent='No matching airport in the available catalogue.';
  }
  function open(code){map.setRotating(false);el('search').value='';el('status').textContent='';render();detail(code);if(!d.open)d.showModal();el('close').focus();}
  el('search').oninput=render;el('close').onclick=()=>d.close();el('terrain').onchange=()=>map.setTerrain(el('terrain').value);
  el('startup').onclick=()=>el('status').textContent=map.saveStartup()?'Startup camera saved on this device.':'Map is still loading.';
  el('favorite').onclick=()=>{if(!selected)return;const fav=map.getFavorites(),has=fav.includes(selected.icao);if(!has&&fav.length>=50){el('status').textContent='Maximum 50 favorites; remove one first.';return;}map.setFavorites(has?fav.filter(c=>c!==selected.icao):[...fav,selected.icao]);render();detail(selected.icao);};
  el('view').onclick=()=>{if(selected){map.viewAirport(selected.icao);d.close();}};
  for(const field of ['departure','destination'])el(field).onclick=async()=>{if(!selected)return;const code=selected.icao;d.close();await addAirport(code,field);};
  return {open};
};

/* Login document only: the server still decides whether authentication is needed. */
(()=>{
  let fresh=true;
  try {fresh=!sessionStorage.getItem('nga-unlock-presented');sessionStorage.setItem('nga-unlock-presented','1');}catch{}
  if(!fresh)return;
  document.documentElement.classList.add('nga-startup');
  const started=performance.now();
  function reveal(){
    document.documentElement.classList.remove('nga-startup');
    clearTimeout(timer);
    document.removeEventListener('keydown',reveal);document.removeEventListener('pointerdown',reveal);
  }
  const timer=setTimeout(reveal,Math.max(0,3000-(performance.now()-started)));
  // Authentication remains usable immediately if the pilot starts interacting.
  document.addEventListener('keydown',reveal,{once:true});document.addEventListener('pointerdown',reveal,{once:true});
  window.addEventListener('pagehide',reveal,{once:true});
})();

/* Appearance only. No flight data or session state is touched. */
(function () {
  const mapTools = document.getElementById('opsMapTools');
  const mapToolsBtn = document.getElementById('opsMapToolsBtn');
  if (mapTools && mapToolsBtn) {
    const expand = on => { mapTools.hidden = !on; mapToolsBtn.setAttribute('aria-expanded',String(on)); };
    expand(false);
    mapToolsBtn.addEventListener('click',() => expand(mapTools.hidden));
    mapTools.addEventListener('keydown',e => {
      if (e.key === 'Escape') { expand(false); mapToolsBtn.focus(); }
    });
  }
  const key = 'nga-efb-material';
  const buttons = document.querySelectorAll('[data-app-material]');
  function apply(value, persist) {
    const material = value === 'solid' ? 'solid' : 'glass';
    document.documentElement.dataset.efbMaterial = material;
    buttons.forEach(button => button.setAttribute('aria-pressed', String(button.dataset.appMaterial === material)));
    if (persist) { try { localStorage.setItem(key, material); } catch {} }
  }
  let saved;
  try { saved = localStorage.getItem(key); } catch {}
  apply(saved, false);
  buttons.forEach(button => button.addEventListener('click', () => apply(button.dataset.appMaterial, true)));
  window.addEventListener('storage', event => { if (event.key === key) apply(event.newValue, false); });
})();

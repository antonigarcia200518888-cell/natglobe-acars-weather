/* Optional decoration only. Never requests device, map, or flight data. */
(() => {
  'use strict';
  const root = document.documentElement;
  const canvas = document.getElementById('glassCanvas');
  const surfaces = Array.from(document.querySelectorAll('[data-glass-surface]'));
  const controls = Array.from(document.querySelectorAll('[data-glass-control]'));
  if (!canvas || !surfaces.length || !window.PilotGlassRenderer) return;
  const preferences = ['(prefers-reduced-transparency: reduce)', '(forced-colors: active)'].map(q => matchMedia(q));
  const reduceMotion = matchMedia('(prefers-reduced-motion: reduce)');
  const permitted = () => root.dataset.efbMaterial !== 'solid' && !preferences.some(q => q.matches);
  let image, renderer, loading = false, lost = false, failed = false, frame = 0, lastPaint = -Infinity;
  const press = { x:0, y:0, amount:0, target:0 };
  function suspend(reason) {
    cancelAnimationFrame(frame); frame = 0; press.amount = press.target = 0;
    delete root.dataset.pilotGlass;
    canvas.dataset.state = reason;
  }
  function invalidate() {
    if (!frame && !document.hidden && !lost && renderer && permitted()) frame = requestAnimationFrame(draw);
  }
  function draw(now) {
    frame = 0;
    if (document.hidden || !permitted() || lost || !renderer) return;
    const dt = now - lastPaint;
    if (dt < renderer.frameInterval) { invalidate(); return; }
    lastPaint = now;
    press.amount += (press.target - press.amount) * (1-Math.exp(-Math.min(dt,80)/55));
    if (Math.abs(press.target - press.amount) < .01) press.amount = press.target;
    if (renderer.paint(root.dataset.efbTheme === 'night', press)) { root.dataset.pilotGlass = 'ready'; canvas.dataset.state = 'ready'; }
    else { suspend('context-lost'); return; }
    if (press.amount !== press.target) invalidate();
  }
  function sync() {
    if (!permitted() || document.hidden || lost || failed) { suspend(failed ? 'unavailable' : lost ? 'context-lost' : document.hidden ? 'paused' : 'preference'); return; }
    if (!image) {
      if (loading) return;
      loading = true;
      const img = new Image();
      img.onload = () => { image = img; loading = false; sync(); };
      img.onerror = () => { failed = true; suspend('texture-unavailable'); };
      img.src = '/pilot-glass-relief.svg';
      return;
    }
    if (!renderer) {
      try { renderer = window.PilotGlassRenderer(canvas, surfaces, controls, image); } catch (err) { console.warn('Pilot glass fallback:',err.message); }
      if (!renderer) { failed = true; suspend('renderer-unavailable'); return; }
    }
    invalidate();
  }
  canvas.addEventListener('webglcontextlost', e => { e.preventDefault(); lost = true; renderer = null; suspend('context-lost'); });
  canvas.addEventListener('webglcontextrestored', () => { lost = failed = false; sync(); });
  for (const el of surfaces) {
    el.addEventListener('pointerdown', e => {
      if (reduceMotion.matches || !e.target.closest('button:not(:disabled)')) return;
      press.x=e.clientX; press.y=e.clientY; press.target=1; invalidate();
    });
    el.addEventListener('scroll',invalidate,{passive:true});
  }
  const release = () => { if (press.target) { press.target=0; invalidate(); } };
  for (const event of ['pointerup','pointercancel','blur']) window.addEventListener(event,release);
  for (const pref of preferences) pref.addEventListener('change',sync);
  reduceMotion.addEventListener('change',() => { press.amount=press.target=0; invalidate(); });
  document.addEventListener('visibilitychange',sync);
  window.addEventListener('resize',invalidate);
  if (window.ResizeObserver) { const observer = new ResizeObserver(invalidate); [...surfaces,...controls].forEach(el => observer.observe(el)); }
  const appearance = new MutationObserver(sync);
  appearance.observe(root,{attributes:true,attributeFilter:['data-efb-material','data-efb-theme']});
  new MutationObserver(invalidate).observe(document.body,{attributes:true,attributeFilter:['class','data-app-workspace']});
  sync();
})();

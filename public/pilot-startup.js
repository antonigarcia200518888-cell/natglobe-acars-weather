/* Login splash: a fresh document or foreground return, never internal routing. */
(() => {
  let timer;
  function show() {
    clearTimeout(timer);
    document.documentElement.classList.add('nga-startup');
    timer = setTimeout(() => {
      if (document.hidden) return;
      document.documentElement.classList.remove('nga-startup');
      document.querySelector('#passkeyLogin')?.focus({ preventScroll: true });
    }, 5000);
  }
  show();
  document.addEventListener('visibilitychange', () => {
    if (window.NGAPilotAuthInProgress) return;
    if (document.hidden) {
      clearTimeout(timer);
      document.documentElement.classList.add('nga-startup');
    } else show();
  });
  window.addEventListener('pageshow', event => { if (event.persisted) show(); });
})();

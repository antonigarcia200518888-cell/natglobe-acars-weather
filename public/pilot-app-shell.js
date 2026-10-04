/* App navigation and presentation only. Operational authority stays on the server. */
window.createPilotAppShell = function createPilotAppShell({ navigate, getFlight, setTheme, install, canInstall }) {
  const node = id => document.getElementById(id);
  const scroller = document.querySelector('.efb-content');
  const titles = { dashboard:'Home', flights:'Flights', 'flight-plan':'Flight briefing', weather:'Weather', performance:'Performance', aircraft:'Aircraft', logbook:'Logbook', documents:'Flight files' };
  const positions = new Map();
  const displayMode = window.matchMedia('(display-mode: standalone)');
  const session = history.state?.ngaAppSession || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
  let current = '';
  let initialized = false;
  let navigation = 0;

  function syncTheme(theme) {
    document.querySelectorAll('[data-app-theme]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.appTheme === theme)));
  }

  function syncDeviceState() {
    const standalone = displayMode.matches || navigator.standalone === true;
    node('appDisplayState').textContent = standalone ? 'Home Screen app' : 'Browser session';
    node('appInstallGuide').hidden = standalone;
    node('appNativeInstallBtn').hidden = standalone || !canInstall();
    node('appConnection').dataset.offline = String(!navigator.onLine);
    node('appConnection').textContent = navigator.onLine ? 'Network' : 'Offline';
    node('appConnection').title = navigator.onLine ? 'Device reports a network connection. Check each data source for freshness.' : 'Device is offline. Live data and server changes are unavailable.';
    node('appNetworkDetail').textContent = navigator.onLine ? 'Network available · live data freshness is shown in each workspace.' : 'Offline · saved snapshots may be stale. Server changes require a connection.';
  }

  function setFocus(enabled) {
    const focused = Boolean(enabled && current === 'flight-plan' && getFlight()?.id);
    document.body.classList.toggle('app-focus', focused);
    node('appFocusBtn').setAttribute('aria-pressed', String(focused));
    node('appFocusBtn').setAttribute('aria-label', focused ? 'Exit briefing focus mode' : 'Focus on flight briefing');
    node('appFocusLabel').textContent = focused ? 'Exit focus' : 'Focus';
  }

  function updateContext() {
    const flight = getFlight();
    node('appWorkspaceTitle').textContent = titles[current] || 'Pilot EFB';
    node('appWorkspaceSubtitle').textContent = flight ? `${flight.dep || '----'} → ${flight.arr || '----'} · ${flight.id}` : 'NGA · Pilot operations';
    node('appFocusBtn').hidden = current !== 'flight-plan' || !flight?.id;
    node('appBackBtn').disabled = history.state?.ngaAppSession !== session || !(history.state?.ngaAppDepth > 0);
    if (current !== 'flight-plan' || !flight?.id) setFocus(false);
  }

  function leave(next) {
    if (current && next !== current) positions.set(current, scroller.scrollTop);
  }

  function enter(view, { historyMode = 'push' } = {}) {
    const changed = view !== current;
    const url = view === 'dashboard' ? location.pathname + location.search : `${location.pathname}${location.search}#${view}`;
    const depth = history.state?.ngaAppSession === session ? Number(history.state.ngaAppDepth) || 0 : 0;
    if (!initialized || historyMode === 'replace') {
      history.replaceState({ ...history.state, ngaAppSession:session, ngaAppDepth:depth, ngaWorkspace:view }, '', url);
    } else if (changed && historyMode !== 'none') {
      history.pushState({ ngaAppSession:session, ngaAppDepth:depth + 1, ngaWorkspace:view }, '', url);
    }
    initialized = true;
    current = view;
    document.body.dataset.appWorkspace = view;
    document.title = `${titles[view] || 'Pilot EFB'} · NGA Pilot EFB`;
    updateContext();
    if (changed) {
      const token = ++navigation;
      window.requestAnimationFrame(() => {
        if (navigation === token) scroller.scrollTo({ top:positions.get(view) || 0, behavior:'auto' });
      });
    }
  }

  function restoreLocation() {
    const hash = location.hash.slice(1);
    const view = Object.hasOwn(titles, hash) ? hash : 'dashboard';
    if (view !== current) navigate(view, { historyMode:'none' });
    else updateContext();
  }

  function openSettings() {
    syncDeviceState();
    syncTheme(document.documentElement.dataset.efbTheme);
    if (!node('appPreferencesDialog').open) node('appPreferencesDialog').showModal();
  }

  function confirmReload() {
    node('appPreferencesDialog').close();
    const dialog = node('appReloadDialog');
    dialog.returnValue = 'cancel';
    if (!dialog.open) dialog.showModal();
  }

  node('appBackBtn').addEventListener('click', () => {
    if (!node('appBackBtn').disabled) history.back();
  });
  node('appFocusBtn').addEventListener('click', () => setFocus(!document.body.classList.contains('app-focus')));
  node('appSettingsBtn').addEventListener('click', openSettings);
  node('appSettingsCloseBtn').addEventListener('click', () => node('appPreferencesDialog').close());
  node('appReloadBtn').addEventListener('click', confirmReload);
  node('appReloadDialog').addEventListener('close', () => {
    if (node('appReloadDialog').returnValue === 'reload') location.reload();
  });
  node('appNativeInstallBtn').addEventListener('click', async () => {
    node('appNativeInstallBtn').disabled = true;
    try { await install(); syncDeviceState(); }
    catch (_) { node('appNetworkDetail').textContent = 'Installation was not completed. You can try again or use the browser menu.'; }
    finally { node('appNativeInstallBtn').disabled = false; }
  });
  document.querySelectorAll('[data-app-theme]').forEach(button => button.addEventListener('click', () => setTheme(button.dataset.appTheme)));
  node('appPreferencesDialog').addEventListener('click', event => {
    if (event.target !== event.currentTarget) return;
    const rect = event.currentTarget.getBoundingClientRect();
    if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) event.currentTarget.close();
  });
  window.addEventListener('popstate', restoreLocation);
  window.addEventListener('hashchange', restoreLocation);
  window.addEventListener('online', syncDeviceState);
  window.addEventListener('offline', syncDeviceState);
  window.addEventListener('appinstalled', syncDeviceState);
  displayMode.addEventListener('change', syncDeviceState);
  syncDeviceState();
  return { leave, enter, updateContext, syncTheme, syncDeviceState, openSettings, confirmReload };
};

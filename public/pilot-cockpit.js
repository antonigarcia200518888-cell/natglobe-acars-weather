/* Small, independently scheduled cockpit displays. No location is persisted or uploaded. */
(function (root) {
  'use strict';
  function positionHealth(fix, now = Date.now()) {
    if (!fix || !Number.isFinite(fix.accuracy) || fix.accuracy < 0 || !Number.isFinite(fix.timestamp)) return { tone:'caution', label:'No fix' };
    if (now - fix.timestamp > 15000 || fix.timestamp > now + 1000) return { tone:'caution', label:'Fix stale' };
    return { tone:fix.accuracy <= 50 ? 'normal' : 'caution', label:`±${Math.ceil(fix.accuracy)} m` };
  }
  function secondDelay(now) { return 1000 - (now % 1000); }
  function retainedFlight(requests, selectedId) {
    return selectedId ? requests.find(item => item.id === selectedId) || null : null;
  }
  root.PilotCockpit = { positionHealth, secondDelay, retainedFlight };
  root.createPilotCockpit = function ({ onMinute = () => {} } = {}) {
    const node = id => document.getElementById(id);
    const clock = node('clock');
    const locationButton = node('cockpitLocationBtn');
    const dialog = node('cockpitLocationDialog');
    let timer = 0, locationTimer = 0, watch = null, wanted = false, fix = null, generation = 0;
    let lastSecond = '', lastMinute = '', failure = '';
    const write = (target, value) => { if (target.textContent !== value) target.textContent = value; };
    function showLocation() {
      const state = failure ? { tone:'caution', label:failure } : wanted ? positionHealth(fix) : { tone:'neutral', label:'Location off' };
      write(node('cockpitLocationValue'), state.label);
      locationButton.dataset.tone = state.tone;
      locationButton.setAttribute('aria-label', `Device location: ${state.label}`);
      locationButton.title = 'Device location accuracy, not GPS integrity. Tap for details.';
      node('cockpitLocationStart').hidden = wanted;
      node('cockpitLocationStop').hidden = !wanted;
      if (!dialog.open) return;
      write(node('cockpitPositionDetail'), fix && !failure
        ? `${fix.latitude.toFixed(4)}°, ${fix.longitude.toFixed(4)}° · reported accuracy ±${Math.ceil(fix.accuracy)} m · ${Math.max(0, Math.floor((Date.now() - fix.timestamp) / 1000))}s old`
        : failure || (wanted ? 'Waiting for a device location…' : 'Location monitoring is off.'));
    }
    function stopWatch() {
      generation++;
      if (watch !== null) navigator.geolocation?.clearWatch(watch);
      watch = null;
      clearTimeout(locationTimer);
      locationTimer = 0;
    }
    function startWatch() {
      if (!wanted || document.hidden || watch !== null) return;
      if (!navigator.geolocation || !window.isSecureContext) { failure = 'Unavailable'; wanted = false; showLocation(); return; }
      const token = ++generation;
      watch = navigator.geolocation.watchPosition(position => {
        if (token !== generation) return;
        const c = position.coords;
        if (![c.latitude, c.longitude, c.accuracy, position.timestamp].every(Number.isFinite) || Math.abs(c.latitude) > 90 || Math.abs(c.longitude) > 180 || c.accuracy < 0) return;
        fix = { latitude:c.latitude, longitude:c.longitude, accuracy:c.accuracy, timestamp:position.timestamp };
        failure = '';
        // Latest-only mailbox: at most five text updates per second, no per-fix tasks.
        if (!locationTimer) locationTimer = setTimeout(() => { locationTimer = 0; showLocation(); }, 200);
      }, error => {
        if (token !== generation) return;
        failure = error.code === 1 ? 'Permission denied' : error.code === 3 ? 'Fix timeout' : 'No fix';
        fix = null;
        if (error.code === 1) { wanted = false; stopWatch(); }
        showLocation();
      }, { enableHighAccuracy:true, maximumAge:0, timeout:15000 });
    }
    function tick() {
      clearTimeout(timer);
      if (document.hidden) return;
      const now = new Date();
      const second = now.toISOString().slice(11, 19);
      if (lastSecond !== second) { write(clock, `${second} Z`); lastSecond = second; }
      const minute = now.toISOString().slice(0, 16);
      if (lastMinute !== minute) { lastMinute = minute; onMinute(); }
      showLocation();
      timer = setTimeout(tick, secondDelay(Date.now()));
    }
    locationButton.addEventListener('click', () => { dialog.showModal(); showLocation(); });
    node('cockpitLocationClose').addEventListener('click', () => dialog.close());
    node('cockpitLocationStart').addEventListener('click', () => { wanted = true; failure = ''; fix = null; startWatch(); showLocation(); });
    node('cockpitLocationStop').addEventListener('click', () => { wanted = false; stopWatch(); fix = null; failure = ''; showLocation(); });
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) { clearTimeout(timer); stopWatch(); }
      else { startWatch(); tick(); }
    });
    window.addEventListener('pagehide', () => { clearTimeout(timer); stopWatch(); });
    window.addEventListener('pageshow', () => { startWatch(); tick(); });
    tick();
    return { refresh:tick };
  };
})(typeof window === 'undefined' ? globalThis : window);

/* A foreground privacy lock backed by a NEW server-verified login.
   The editor is kept in memory; no flight record is changed by locking.
   Offline unlock is deliberately not simulated. WebAuthn needs a server challenge. */
(() => {
  if (window.top !== window) return; // Embedded planners inherit their EFB host's lock.
  const root = document.documentElement;
  root.classList.add('nga-session-pending');
  let resolveReady, dialog, status, unlock, form, previousFocus, previousProfile;
  let locked = true, authenticating = false, generation = 0, timer, cleared = false, clearing;
  let ready = new Promise(resolve => { resolveReady = resolve; });
  window.NGAPilotLock = {
    get locked() { return locked; }, get ready() { return ready; }, lock: () => lock()
  };
  const json = async (url, options = {}) => {
    const response = await fetch(url, { cache: 'no-store', ...options, signal: AbortSignal.timeout(15000) });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw Error(data.error || 'Unable to reach Pilot Ops. Reconnect and try again.');
    return data;
  };
  function protect() {
    root.classList.add('nga-session-pending');
    if (dialog && !dialog.open) dialog.showModal();
    if (dialog) dialog.dataset.phase = 'splash';
  }
  function lock() {
    if (authenticating) return; // The OS authentication sheet is not an app departure.
    if (!locked) {
      previousFocus = document.activeElement;
      ready = new Promise(resolve => { resolveReady = resolve; });
      cleared = false; clearing = null;
    }
    locked = true; generation++;
    clearTimeout(timer); protect();
    if (!document.hidden && dialog) present();
  }
  async function present() {
    const current = ++generation;
    protect();
    unlock.disabled = true;
    dialog.querySelector('[data-retry]').hidden = true;
    form.querySelector('button').disabled = true;
    status.textContent = 'Secure unlock required. Your open flight remains in place.';
    clearTimeout(timer);
    timer = setTimeout(() => {
      if (current !== generation || document.hidden) return;
      dialog.dataset.phase = 'unlock'; unlock.focus({ preventScroll: true });
    }, 5000);
    try {
      // Rapid visibility changes must not send a late logout after a new login.
      if (!clearing) clearing = (async () => {
        if (!previousProfile) {
          const session = await json('/api/pilot-session').catch(() => null);
          previousProfile = session?.session || null;
        }
        await json('/api/pilot-logout', { method: 'POST' });
      })().catch(error => { clearing = null; throw error; });
      await clearing;
      if (current !== generation) return;
      cleared = true;
      unlock.disabled = !window.PublicKeyCredential;
      form.querySelector('button').disabled = false;
    } catch {
      if (current !== generation) return;
      status.textContent = 'Connection required to verify your identity. Reconnect, then tap Retry connection.';
      dialog.querySelector('[data-retry]').hidden = false;
    }
  }
  async function authenticate(action) {
    if (authenticating || !cleared || document.hidden) return;
    authenticating = true; unlock.disabled = true; form.querySelector('button').disabled = true;
    const current = generation;
    status.textContent = 'Confirm with Face ID, Touch ID, device PIN, or your pilot access code.';
    try {
      const result = await action();
      if (current !== generation || document.hidden) {
        status.textContent = 'Return to Pilot Ops and unlock again.'; return;
      }
      if (previousProfile && (result.role !== previousProfile.role || result.name !== previousProfile.name)) {
        status.textContent = 'Use the same pilot profile to resume this workspace.';
        await json('/api/pilot-logout', { method: 'POST' }); return;
      }
      previousProfile = result;
      locked = false;
      root.classList.remove('nga-session-pending'); dialog.close();
      form.reset(); resolveReady();
      previousFocus?.focus({ preventScroll: true });
      window.dispatchEvent(new CustomEvent('nga-pilot-unlocked'));
    } catch (error) {
      status.textContent = error.name === 'NotAllowedError' || error.name === 'AbortError'
        ? 'Unlock cancelled. Tap Face ID / Touch ID to try again, or use your pilot code.'
        : error.message || 'Unlock failed. Your workspace remains locked.';
      dialog.querySelector('[data-retry]').hidden = false;
    } finally {
      authenticating = false; unlock.disabled = !window.PublicKeyCredential;
      form.querySelector('button').disabled = false;
      if (document.hidden) lock();
    }
  }
  function mount() {
    dialog = document.createElement('dialog'); dialog.className = 'pilot-session-lock';
    dialog.setAttribute('aria-labelledby', 'pilotUnlockTitle');
    dialog.innerHTML = `<div class="pilot-lock-splash"><img src="/pilot-natglobe-logo.png" width="1414" height="517" alt="NatGlobe Aviation"></div>
      <section class="pilot-lock-panel"><img src="/pilot-natglobe-logo.png" width="1414" height="517" alt="NatGlobe Aviation">
      <h1 id="pilotUnlockTitle">Unlock Pilot Ops</h1><p>Your flight and unsaved work are kept in place.</p>
      <button type="button" data-unlock>Use Face ID / Touch ID</button>
      <p class="pilot-lock-note">Your device may offer its PIN. Set up a passkey in Profile after signing in with your pilot code.</p>
      <details><summary>Use pilot access code</summary><form><label>Pilot access code<input type="password" autocomplete="current-password" required></label><button type="submit">Unlock with code</button></form></details>
      <p role="status" aria-live="polite" data-status></p><button type="button" data-retry>Retry connection</button>
      </section>`;
    document.body.append(dialog);
    status = dialog.querySelector('[data-status]'); unlock = dialog.querySelector('[data-unlock]'); form = dialog.querySelector('form');
    dialog.addEventListener('cancel', event => event.preventDefault());
    dialog.querySelector('[data-retry]').onclick = () => { if (!authenticating) present(); };
    form.onsubmit = event => {
      event.preventDefault();
      authenticate(() => json('/api/pilot-login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code: form.querySelector('input').value }) }));
    };
    unlock.onclick = () => authenticate(async () => {
      if (!window.SimpleWebAuthnBrowser?.startAuthentication) throw Error('Passkeys unavailable. Use your pilot access code.');
      const options = await json('/api/pilot-passkeys/authentication/options', { method: 'POST' });
      const credential = await window.SimpleWebAuthnBrowser.startAuthentication({ optionsJSON: options.options });
      return json('/api/pilot-passkeys/authentication/verify', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ transactionId: options.transactionId, credential }) });
    });
    let handoff = 0;
    try { handoff = Number(sessionStorage.getItem('nga-pilot-login-handoff')); sessionStorage.removeItem('nga-pilot-login-handoff'); } catch {}
    // One-use UI handoff avoids asking twice immediately after the login page.
    // Both this page and the check still require the server-signed HttpOnly cookie.
    if (handoff > 0 && Date.now() - handoff >= 0 && Date.now() - handoff < 15000) {
      const current = generation;
      json('/api/pilot-session').then(data => {
        if (current !== generation || document.hidden) return;
        previousProfile = data.session; locked = false;
        root.classList.remove('nga-session-pending'); resolveReady();
      }).catch(() => lock());
    } else lock();
  }
  document.addEventListener('visibilitychange', () => {
    if (authenticating) return;
    if (document.hidden) lock(); else if (locked && dialog) present();
  });
  window.addEventListener('pagehide', () => { if (!authenticating) lock(); });
  window.addEventListener('pageshow', event => { if (event.persisted) lock(); });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount, { once: true }); else mount();
})();

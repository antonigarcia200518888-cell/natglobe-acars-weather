/* Flight selection UI only. Saving and release validation remain server-controlled. */
window.pilotFlightMatches = function pilotFlightMatches(flight, query = '', scope = 'ALL', today = '') {
  const words = String(query).trim().toUpperCase().split(/\s+/).filter(Boolean);
  const text = [flight.id, flight.dep, flight.arr, flight.depName, flight.arrName, flight.route,
    flight.aircraft, flight.requestDate, flight.requestTime, flight.crew?.commander].join(' ').toUpperCase();
  return words.every(word => text.includes(word))
    && (scope !== 'TODAY' || flight.requestDate === today)
    && (scope !== 'MANUAL' || flight.manualFlight === true);
};

window.createPilotFlightDeck = function createPilotFlightDeck({ getFlights, getSelected, getPlanner, choose, create, today, escapeHtml }) {
  const node = id => document.getElementById(id);
  const picker = node('flightPickerDialog');
  const guard = node('flightChangeDialog');
  let scope = 'ALL';
  let pending = false;
  let settle = null;
  let saving = false;

  function render() {
    const selected = getSelected();
    const flights = getFlights().filter(flight => window.pilotFlightMatches(flight, node('flightPickerSearch').value, scope, today()));
    node('flightPickerCount').textContent = `${flights.length} flight${flights.length === 1 ? '' : 's'}`;
    document.querySelectorAll('[data-flight-scope]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.flightScope === scope)));
    node('flightPickerResults').innerHTML = flights.map(flight => {
      const active = flight.id === selected?.id;
      return `<button type="button" class="flight-picker-row" data-pick-flight="${escapeHtml(flight.id)}" aria-current="${active ? 'true' : 'false'}">
        <span class="flight-picker-route"><strong>${escapeHtml(flight.dep || '----')} <span aria-hidden="true">→</span> ${escapeHtml(flight.arr || '----')}</strong><em>${active ? 'ACTIVE' : flight.manualFlight ? 'PILOT FILE' : escapeHtml(flight.status || 'BOOKING')}</em></span>
        <span class="flight-picker-schedule">${escapeHtml(flight.requestDate || 'Date not set')} · ${escapeHtml(flight.requestTime || '--:--')}L <span>${escapeHtml(flight.aircraft || 'Aircraft not set')}</span></span>
        <small>${escapeHtml(flight.id)}</small></button>`;
    }).join('') || '<div class="flight-picker-empty"><strong>No matching flights</strong><p>Try an airport, flight reference, date or aircraft. The filters only change this list.</p></div>';
  }

  function open() {
    node('flightPickerSearch').value = '';
    scope = 'ALL';
    render();
    picker.showModal();
    // Do not force the iPad keyboard open just to select a flight.
    node('flightPickerCloseBtn').focus();
  }

  function updateDraftStatus() {
    const state = getPlanner()?.getState();
    const badge = node('appDraftBadge');
    badge.hidden = !state?.dirty;
    badge.textContent = state?.busy ? 'Saving OFP…' : 'Unsaved OFP';
    if (guard.open && !saving) node('flightChangeSaveBtn').disabled = Boolean(state?.busy);
  }

  async function ensureSaved() {
    if (pending) return false;
    const planner = getPlanner();
    const state = planner?.getState();
    if (!state?.dirty && !state?.busy) return true;
    pending = true;
    const outcome = new Promise(resolve => { settle = resolve; });
    node('flightChangeMessage').textContent = state.busy
      ? 'The current OFP is saving. Wait for it to finish before continuing.'
      : `There are unsaved OFP edits for ${state.requestId}. Save them before continuing, or return to the current flight.`;
    node('flightChangeSaveBtn').disabled = Boolean(state.busy);
    guard.showModal();
    return outcome;
  }

  function finish(allowed) {
    const resolve = settle;
    settle = null;
    pending = false;
    guard.close();
    resolve?.(allowed);
  }

  node('flightSwitchBtn').addEventListener('click', open);
  node('flightPickerCloseBtn').addEventListener('click', () => picker.close());
  node('flightPickerSearch').addEventListener('input', render);
  document.querySelectorAll('[data-flight-scope]').forEach(button => button.addEventListener('click', () => { scope = button.dataset.flightScope; render(); }));
  node('flightPickerResults').addEventListener('click', event => {
    const button = event.target.closest('[data-pick-flight]');
    if (!button) return;
    const flight = getFlights().find(item => item.id === button.dataset.pickFlight);
    if (!flight) return;
    picker.close();
    choose(flight);
  });
  node('flightPickerCreateBtn').addEventListener('click', () => { picker.close(); create(); });
  node('flightChangeCancelBtn').addEventListener('click', () => finish(false));
  guard.addEventListener('cancel', event => { event.preventDefault(); if (!saving) finish(false); });
  guard.addEventListener('close', () => { if (settle) finish(false); });
  node('flightChangeSaveBtn').addEventListener('click', async () => {
    if (saving) return;
    const planner = getPlanner();
    if (!planner || planner.getState().busy) return;
    saving = true;
    node('flightChangeSaveBtn').disabled = true;
    node('flightChangeCancelBtn').disabled = true;
    node('flightChangeMessage').textContent = 'Saving your current OFP…';
    try {
      await planner.save();
      if (planner.getState().dirty) throw new Error('New edits were made while saving. Save again to include them.');
      finish(true);
    } catch (error) {
      node('flightChangeMessage').textContent = `${error.message || 'Unable to save the OFP.'} Your current flight remains open.`;
    } finally {
      saving = false;
      node('flightChangeSaveBtn').disabled = false;
      node('flightChangeCancelBtn').disabled = false;
      updateDraftStatus();
    }
  });
  return { open, render, ensureSaved, updateDraftStatus };
};

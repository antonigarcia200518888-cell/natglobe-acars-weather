/* Progressive enhancement: keep each original select as the existing data binding.
   Native selects remain usable if dialog support or this script is unavailable. */
(function () {
  'use strict';
  if (!window.HTMLDialogElement) return;
  const controls = new Map();
  const sheet = document.createElement('dialog');
  sheet.className = 'pilot-choice-sheet';
  sheet.setAttribute('aria-labelledby', 'pilotChoiceTitle');
  sheet.innerHTML = '<header><div><small>CHOOSE AN OPTION</small><h2 id="pilotChoiceTitle"></h2></div><button type="button" class="pilot-choice-close">Cancel</button></header><input class="pilot-choice-search" type="search" aria-label="Filter options" placeholder="Find an option"><div class="pilot-choice-list"></div><p class="pilot-choice-empty" hidden>No matching options</p>';
  document.body.append(sheet);
  const title = sheet.querySelector('h2');
  const search = sheet.querySelector('input');
  const list = sheet.querySelector('.pilot-choice-list');
  let current = null, opener = null, frame = 0;
  function label(select) {
    return select.getAttribute('aria-label') || [...(select.labels || [])].map(item => [...item.childNodes].filter(n => n.nodeType === 3 || n.tagName === 'SPAN').map(n => n.textContent).join(' ').trim()).filter(Boolean).join(' ') || select.name || 'Choose';
  }
  function choose(select, value) {
    if (select.disabled) return;
    const option = [...select.options].find(item => item.value === value);
    if (!option || option.disabled || option.parentElement?.disabled) return;
    if (select.value !== value) {
      select.value = value;
      // Existing select bindings listen to change (some also listen to input).
      // Send it once, avoiding a duplicate full OFP calculation/render pass.
      select.dispatchEvent(new Event('change', { bubbles:true }));
    }
    sync();
  }
  function renderOptions() {
    list.replaceChildren();
    if (!current) return;
    const query = search.value.toLocaleLowerCase().trim();
    for (const option of current.options) {
      if (option.hidden || (query && !option.textContent.toLocaleLowerCase().includes(query))) continue;
      const row = document.createElement('button');
      row.type = 'button';
      row.textContent = option.textContent;
      row.setAttribute('aria-pressed', String(option.selected));
      row.disabled = current.disabled || option.disabled || Boolean(option.parentElement?.disabled);
      row.addEventListener('click', () => { choose(current, option.value); sheet.close(); });
      list.append(row);
    }
    sheet.querySelector('.pilot-choice-empty').hidden = list.childElementCount > 0;
  }
  function open(select, button) {
    current = select; opener = button; title.textContent = label(select); search.value = '';
    search.hidden = select.options.length < 8;
    renderOptions(); sheet.showModal();
    (list.querySelector('[aria-pressed="true"]') || list.querySelector('button') || sheet.querySelector('button')).focus();
  }
  function sync() {
    for (const [select, control] of controls) {
      control.hidden = select.hidden;
      if (control.classList.contains('pilot-segment')) {
        [...control.children].forEach(button => { button.disabled = select.disabled; button.setAttribute('aria-pressed', String(select.value === button.dataset.value)); });
      } else {
        const value = select.selectedOptions[0]?.textContent || 'Choose';
        if (control.textContent !== value) control.textContent = value;
        control.disabled = select.disabled;
        control.setAttribute('aria-label', `${label(select)}: ${value}`);
      }
    }
  }
  function scheduleSync() { if (!frame) frame = requestAnimationFrame(() => { frame = 0; sync(); }); }
  function enhance() {
    document.querySelectorAll('select:not([multiple]):not([data-native-select])').forEach(select => {
      if (controls.has(select)) return;
      const segmented = select.dataset.field === 'flightRules';
      const control = document.createElement(segmented ? 'div' : 'button');
      if (segmented) {
        control.className = 'pilot-segment'; control.setAttribute('role', 'group'); control.setAttribute('aria-label', label(select));
        for (const option of select.options) {
          const button = document.createElement('button'); button.type = 'button'; button.textContent = option.textContent; button.dataset.value = option.value;
          button.addEventListener('click', () => choose(select, option.value)); control.append(button);
        }
      } else {
        control.type = 'button'; control.className = 'pilot-choice-trigger'; control.setAttribute('aria-haspopup', 'dialog');
        control.addEventListener('click', () => open(select, control));
      }
      select.after(control);
      select.classList.add('pilot-enhanced-select');
      select.setAttribute('aria-hidden', 'true'); select.tabIndex = -1;
      select.addEventListener('invalid', event => { event.preventDefault(); control.focus(); });
      new MutationObserver(scheduleSync).observe(select, { attributes:true, childList:true, subtree:true });
      controls.set(select, control);
    });
    sync();
  }
  sheet.querySelector('.pilot-choice-close').addEventListener('click', () => sheet.close());
  sheet.addEventListener('close', () => { current = null; opener?.focus(); });
  search.addEventListener('input', renderOptions);
  document.addEventListener('change', scheduleSync);
  document.addEventListener('pilot-ui-sync', scheduleSync);
  // Select values may be set by existing async model renders. Their DOM changes
  // schedule one coalesced sync; never run a polling loop or observe our own output.
  new MutationObserver(records => {
    if (records.some(record => !record.target.closest?.('.pilot-choice-sheet,.pilot-choice-trigger,.pilot-segment') && record.target.id !== 'clock')) scheduleSync();
  }).observe(document.body, { childList:true, subtree:true });
  window.pilotTouchControls = { sync:scheduleSync, enhance };
  enhance();
})();

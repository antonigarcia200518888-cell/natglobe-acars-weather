/* Touch presentation around the existing OFP model, not a second draft store. */
(function () {
  const node = id => document.getElementById(id);
  const toggle = node('ofpSectionsToggle');
  toggle.addEventListener('click', () => {
    const collapsed = document.body.classList.toggle('ofp-sections-collapsed');
    toggle.setAttribute('aria-expanded', String(!collapsed));
    toggle.setAttribute('aria-label', collapsed ? 'Show flight sections' : 'Hide flight sections');
  });
  document.querySelectorAll('[data-planning-target]').forEach(button => button.addEventListener('click', () => node(button.dataset.planningTarget)?.click()));
  const remarks = document.querySelector('[data-field="briefingRemarks"]');
  const sheet = document.createElement('dialog');
  sheet.className = 'ofp-notes-sheet';
  sheet.setAttribute('aria-labelledby', 'ofpNotesTitle');
  sheet.innerHTML = '<header><div><small>FLIGHT SCRATCHPAD</small><h2 id="ofpNotesTitle">Briefing notes</h2></div><button type="button" data-cancel>Cancel</button></header><p>Clearances, weather and operational notes. Apply to the draft, then Save to keep them with this flight.</p><textarea aria-label="Flight scratchpad" spellcheck="false" placeholder="Write a clearance or briefing note…"></textarea><p data-status role="status"></p><footer><button type="button" data-apply>Apply to draft</button></footer>';
  document.body.append(sheet);
  const input = sheet.querySelector('textarea');
  let original = '';
  node('ofpScratchpadBtn').addEventListener('click', () => {
    original = remarks.value;
    input.value = original;
    sheet.querySelector('[data-status]').textContent = '';
    sheet.showModal(); input.focus();
  });
  sheet.querySelector('[data-cancel]').addEventListener('click', () => sheet.close());
  sheet.querySelector('[data-apply]').addEventListener('click', () => {
    if (remarks.value !== original) {
      sheet.querySelector('[data-status]').textContent = 'The briefing notes changed while this sheet was open. Copy your text and reopen Notes to merge the changes.';
      return;
    }
    if (input.value !== original) {
      remarks.value = input.value;
      remarks.dispatchEvent(new Event('input', { bubbles:true }));
      remarks.dispatchEvent(new Event('change', { bubbles:true }));
    }
    sheet.close();
  });
  sheet.addEventListener('close', () => node('ofpScratchpadBtn').focus());
  // Swipe reveals the same existing remove action; it never silently deletes a fix.
  let swipe = null;
  node('routeTokens').addEventListener('pointerdown', event => {
    const token = event.target.closest('.route-token');
    swipe = token && !event.target.closest('button') ? { token, x:event.clientX, y:event.clientY, id:event.pointerId } : null;
  });
  node('routeTokens').addEventListener('pointerup', event => {
    if (swipe?.id === event.pointerId && swipe.token.isConnected && swipe.x - event.clientX > 55 && Math.abs(swipe.y - event.clientY) < 30) {
      swipe.token.classList.add('route-remove-revealed');
      swipe.token.querySelector('button')?.focus({ preventScroll:true });
    }
    swipe = null;
  });
  node('routeTokens').addEventListener('pointercancel', () => { swipe = null; });
})();

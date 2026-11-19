/* Move existing controls, never clone fields or create a second flight model. */
(() => {
  const schedule = document.getElementById('scheduleInputs');
  for (const selector of ['.date-field', '.local-time-field', '.time-conversion-panel', '.ofp-schedule-card']) {
    const control = document.querySelector(selector);
    if (control) schedule.append(control);
  }
  const summary = document.getElementById('flightSummarySection');
  const review = document.createElement('details'); review.className = 'overview-snapshot';
  const heading = document.createElement('summary'); heading.textContent = 'Review flight summary & readiness';
  review.append(heading, summary); summary.hidden = false;
  document.querySelector('#flightSetupSection > .detail-body').append(review);
  // Seven calculated clock fields duplicated the same timeline. Retain their
  // bindings for existing calculations/PDFs, but show one authoritative table.
  document.querySelector('.schedule-drawer').hidden = true;
  document.querySelector('.ofp-planning-segments').hidden = true;
  document.querySelectorAll('.ofp-workspace-button').forEach((button, index) => {
    button.dataset.step = String(index + 1);
  });
})();

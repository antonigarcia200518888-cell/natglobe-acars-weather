/* Presentation and navigation only. Calculations and PIC release remain authoritative elsewhere. */
window.pilotOFPChecks = (() => {
  const field = name => `[data-field="${name}"]`;
  const target = (workspace, selector, label, gate) => ({ workspace, selector, label, gate });

  function describeWarning(message) {
    const value = String(message || '');
    let action;
    if (/runway identifiers/i.test(value)) action = target('SETUP', field('depRunway'), 'Runways', 'route');
    else if (/alternate review/i.test(value)) action = target('SETUP', field('alternate'), 'Alternate airport', 'route');
    else if (/four-character ICAO/i.test(value)) action = target('SETUP', field('departure'), 'Departure & destination', 'setup');
    else if (/occurs twice/i.test(value)) action = target('SETUP', field('departureTimeOccurrence'), 'Departure time occurrence', 'setup');
    else if (/local departure date/i.test(value)) action = target('SETUP', field('dateUtc'), 'Departure date', 'setup');
    else if (/local departure time/i.test(value)) action = target('SETUP', field('scheduledOutLocal'), 'Departure time', 'setup');
    else if (/elapsed flight time/i.test(value)) action = target('SETUP', field('estimatedEnrouteMinutes'), 'Planned flight time', 'setup');
    else if (/flight reference/i.test(value)) action = target('SETUP', field('flightReference'), 'Flight reference', 'setup');
    else if (/pilot route/i.test(value)) action = target('ROUTE', field('route'), 'Route description', 'route');
    else if (/crew weights/i.test(value)) action = target('LOAD', field('crew1Lb'), 'Crew weights', 'load');
    else if (/^Fuel exceeds/i.test(value)) action = target('LOAD', field('tripFuelGal'), 'Fuel plan', 'load');
    else if (/baggage|weight|CG envelope/i.test(value)) action = target('LOAD', '#loadModeSelect', 'Weight & balance', 'load');
    else if (/NOTAM/i.test(value)) action = target('BRIEF', field('notamReviewAccepted'), 'NOTAM review', 'review');
    else if (/airspace/i.test(value)) action = target('BRIEF', field('airspaceReviewAccepted'), 'Airspace review', 'review');
    else if (/performance/i.test(value)) action = target('BRIEF', '#openPerformanceWorkspaceBtn', 'Aircraft performance', 'performance');
    else action = target('BRIEF', '#dispatchSection', 'Review check', 'other');
    return { message:value, ...action };
  }

  function build(warnings, progress) {
    const checks = [...new Set(warnings || [])].map(describeWarning);
    const fallbacks = [
      ['setupReady', 'setup', 'Confirm the flight identity, airports and departure schedule.', target('SETUP', field('aircraftRegistration'), 'Flight setup', 'setup')],
      ['routeReady', 'route', 'Review the route, runways and alternate airport.', target('ROUTE', field('route'), 'Operational route', 'route')],
      ['loadReady', 'load', 'Verify a positive block fuel quantity, actual crew weights and loading limits.', target('LOAD', field('tripFuelGal'), 'Fuel & loading', 'load')],
      ['reviewReady', 'review', 'Complete the NOTAM and airspace review acknowledgements.', target('BRIEF', field('notamReviewAccepted'), 'Briefing review', 'review')],
      ['performanceReady', 'performance', 'Review departure and arrival performance in the dedicated workspace.', target('BRIEF', '#openPerformanceWorkspaceBtn', 'Aircraft performance', 'performance')]
    ];
    for (const [key, gate, message, action] of fallbacks) {
      if (!progress[key] && !checks.some(check => check.gate === gate)) checks.push({message, ...action});
    }
    const order = ['setup', 'route', 'load', 'review', 'performance', 'other'];
    return checks.sort((a, b) => order.indexOf(a.gate) - order.indexOf(b.gate));
  }

  function serverState(panel, saved, released) {
    if (!saved) return { state:'stale', title:'Save to refresh release checks', detail:'Current edits are not included below. Use Save after reviewing your changes.' };
    if (!panel) return { state:'unavailable', title:'Release checks unavailable', detail:'Reload this flight before attempting PIC release.' };
    if (released) return { state:'released', title:'PIC release recorded', detail:'Review the release record below. Planning checks do not replace pilot verification.' };
    if (panel.ready === true) return { state:'ready', title:'Server checks complete · not released', detail:'The commander must still review and explicitly record PIC release.' };
    return { state:'blocked', title:'Server release checks remain open', detail:'Resolve the saved-flight checks below, then save and review again.' };
  }

  function serverAction(message) {
    const actions = {
      'COMPLETE FLIGHT REFERENCE AND SCHEDULE': target('SETUP', field('dateUtc'), 'Flight setup'),
      'COMPLETE ROUTE AND RUNWAYS': target('SETUP', field('depRunway'), 'Runways & airports'),
      'ASSIGN THE COMMANDER': { module:'flights', detailTab:'crew', label:'Crew assignment' },
      'WEATHER OPEN': { module:'flights', detailTab:'release', label:'Flight checks' },
      'NOTAM OPEN': { module:'flights', detailTab:'release', label:'Flight checks' },
      'AIRSPACE OPEN': { module:'flights', detailTab:'release', label:'Flight checks' },
      'PERFORMANCE OPEN': { module:'performance', label:'Performance workspace' },
      'WEIGHT BALANCE OPEN': { module:'flights', detailTab:'release', label:'Flight checks' },
      'FUEL OPEN': { module:'flights', detailTab:'release', label:'Fuel & flight checks' },
      'DOCUMENTS OPEN': { module:'documents', label:'Flight documents' },
      'AIRCRAFT OPEN': { module:'aircraft', label:'Aircraft status' }
    };
    return Object.prototype.hasOwnProperty.call(actions, message) ? actions[message] : null;
  }
  return { describeWarning, build, serverState, serverAction };
})();

/* ForeFlight Mobile route handoff. This only builds a user-opened map URL. */
(() => {
  function buildRouteLink(values = {}) {
    const departure = String(values.departure || '').trim().toUpperCase();
    const destination = String(values.destination || '').trim().toUpperCase();
    if (!/^[A-Z0-9]{4}$/.test(departure) || !/^[A-Z0-9]{4}$/.test(destination)) {
      return { error: 'Enter departure and destination ICAO codes first.' };
    }
    const route = String(values.route || '').trim().toUpperCase();
    if (!route || route.length > 2000 || !/^[A-Z0-9./+@\s-]+$/.test(route)) {
      return { error: 'Enter a route before opening it in ForeFlight.' };
    }
    const tokens = route.split(/\s+/);
    if (tokens[0].split('/')[0] !== departure) tokens.unshift(departure);
    if (tokens[tokens.length - 1].split('/')[0] !== destination) tokens.push(destination);
    const notes = [];
    const speed = Number(values.cruiseSpeedKt);
    if (Number.isFinite(speed) && speed > 0) tokens.push(`${speed}kts`);
    const altitude = String(values.cruiseFlightLevel || '').trim().toUpperCase().replace(/\s+/g, '');
    const flightLevel = altitude.match(/^FL(\d{2,3})$/);
    const feet = altitude.match(/^(\d{1,5})FT$/);
    const metres = altitude.match(/^(\d{1,5})M$/);
    if (flightLevel) tokens.push(`${Number(flightLevel[1]) * 100}ft`);
    else if (feet) tokens.push(`${Number(feet[1])}ft`);
    else if (metres) tokens.push(`${Number(metres[1])}m`);
    else if (altitude && altitude !== 'VFR') notes.push('Altitude was not included. Use FL055, 5500FT or 1500M to include it.');
    const aircraft = String(values.aircraftRegistration || '').trim().toUpperCase();
    if (/^[A-Z0-9][A-Z0-9-]{1,11}$/.test(aircraft)) tokens.push(aircraft);
    const query = tokens.join(' ');
    return { query, url: `foreflightmobile://maps/search?q=${encodeURIComponent(query)}`, notes };
  }
  window.NGAForeFlight = Object.freeze({ buildRouteLink });
})();

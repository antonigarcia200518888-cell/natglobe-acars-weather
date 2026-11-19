# Cockpit, OFP and weather upgrade — 19 November 2026

Asset release: `2026-11-19-1`; service-worker shell cache: **v43**. This continues the [return-lock and flight-document increment](pilot-return-and-flight-documents.md). The application remains Express with vanilla HTML/CSS/JavaScript, MapLibre 5.24, Leaflet 1.9.4 and pdf-lib. No new dependencies, paid services, native iOS project or database migration were introduced.

## Architecture and delivery

1. Keep the existing saved flight record, calculator, readiness gates and PDF generators. Move existing OFP controls rather than cloning them or creating another draft model.
2. Put presentation-only layout, the weather inbox, radar controller and provider clients in separate modules.
3. Use bounded server-side weather caching/batching and client cancellation/generation guards to avoid duplicate or stale updates.
4. Keep device position, selected map point and imported/replay tracks in separate map sources. Never auto-center on a location update.
5. Verify saved-record behavior, provider responses and responsive layouts before publishing through the existing GitHub/Render path.

## What changed

### OFP and flight documents

- Six sections: **Flight overview; Route & alternates; Flight schedule; Fuel & performance; Weather & review; Flight documents**.
- Removed the redundant summary tab; its existing content is available as an Overview disclosure. Legacy saved SUMMARY/PLAN section selections resolve to Overview.
- Moved departure date, local time, time-zone conversion and planned duration together into Schedule. Wide editors show schedule entry beside the UTC movement timeline. Actual dated movements remain separate; existing calculation rules are unchanged.
- Removed duplicate visible computed-clock controls and document actions while retaining required bindings. The duration graphic is optional; the exact movement table remains available.
- Readiness warnings now lead to the moved Schedule controls. Alternate-airport selection does not clear departure/arrival procedures.
- Larger wrapping section buttons, consistent touch controls, adaptive numeric grids, and correctly sized alternate input prevent the observed clipping. Footer actions occupy their own layout area rather than covering the editor.
- OFP and Courier New Journey Log remain in one reader. The inline reader now also exposes Back to flight plan; expanded view keeps Back, Back to EFB and Escape behavior. Existing flight and PDF data are reused.
- Flight-list imports and secondary classification/sort filters are collapsed by default, making the queue visible on smaller landscape screens. Reset filters now also resets flight classification.

### Weather workspace

- Five distinct areas: **Overview / Airports / Radar / Route weather / Requests**. Landscape uses a compact setup sidebar and a separate content area; portrait stacks them.
- Four-letter airport entry, deliberate Gather → Review → Generate actions, raw/decoded controls, source labels, retrieval/observation times and nearby-reference warnings.
- Fast exact-station METAR/TAF/summary requests do **not** silently substitute another airport. The session-only inbox retains the latest 20 requests, with raw Courier New text, partial decoded summaries, request duration, cache status, cancellation and retry.
- NOAA AWC requests batch for 40 ms, share in-flight work, and start no more often than every 700 ms per process. Cache limits: 512 entries, 90 seconds for reports, 30 seconds for no-data, 10 seconds for errors; pending work and queue delay are bounded. The 6.5-second provider timeout includes reading the response body. Missing data and provider errors remain different states. The service is based on the [AWC API](https://aviationweather.gov/data/api/).
- The main briefing endpoint now honors `includeWindsAloft=false`. A serial alternate-weather search no longer blocks every dashboard briefing refresh. Pilot alternate selection and the existing dispatch/PDF behavior remain available.
- Editing airport inputs aborts obsolete work and clears route-specific displays. Manual weather for different airports no longer marks the selected flight as briefed. Report options require a new Gather/Generate action to apply.
- The weather PDF stays inside its existing reader with a visible Back to Weather header; a new 25-second timeout exposes Retry instead of leaving an indefinite spinner.

### Radar

- Operational radar uses FMI's `Radar:radar_finland_cappi_dbzh` WMS layer, its provider legend and the 600 m elevation product. It shows up to 25 available observed frames, not invented nowcasts. Current product details are documented by [FMI](https://www.ilmatieteenlaitos.fi/avoin-data-saatutkat).
- Previous/next, one-pass Play recent, Latest, Fit route, Refresh, opacity, and route/airport visibility controls are connected. Playback waits for tiles, pauses when hidden or panned, and ends on the latest observation. Only visible tiles are requested; there is no bulk prefetch.
- Frame UTC and latest-observation age stay visible. Observations older than 20 minutes are marked stale. Incomplete tile loads are reported; an existing good frame is retained when possible.
- Metadata is shared/cached for two minutes; three tile-layer objects are retained. The app credits FMI and its [CC BY 4.0 terms](https://www.ilmatieteenlaitos.fi/avoin-data-lisenssi). Provider quotas still apply (10,000 WMS requests/day and 600 combined requests/five minutes per user); this is not an unlimited service.

### Position and aircraft presentation

- Opt-in device position has its own blue point, geodesic accuracy circle, update UTC and reported uncertainty in metres. Stale fixes turn gray; explicit Center on device rejects unavailable or stale fixes. Location updates do not pull the map away from the pilot's view.
- Tapped points are amber and labelled **Selected point · not device**. Imported tracks/replay remain separate. Location remains local to the device and monitoring pauses in the background.
- Airfield detail now supports departure, destination **and alternate** assignment. Startup globe, terrain, favorites and saved camera behavior are retained; an available favorite can provide the initial regional focus.
- Existing aircraft artwork is integrated into a bright white hangar-style profile, with corrected image sizing and no dark-mode tint/clipping. This is a static aircraft presentation, **not** a new 3D model or synthetic-vision system.
- Day-mode selected controls now use theme contrast tokens; radar timestamps no longer inherit dark blue text on a dark surface.

## Verification

- `node --test tests/*.test.mjs`: **131 passed, zero failed**, including the temporary PGlite-backed persistence tests. Tests cover batching, cache expiry, errors, radar metadata, position geometry, saved-flight boundaries, document behavior, Schedule warning targets and PDF timeout/return.
- `node --check server.js`: passed. Inline scripts and new browser modules parse in the regression suite. `git diff --check`: passed. This repository has no separate build/lint/TypeScript command; no such build result is claimed.
- `tests/smoke-flight-documents.mjs`, isolated localhost server without `DATABASE_URL`: invalid dates/order rejected; overnight UTC movements verified; **3 OFP pages and 1 Journey Log page** match preview/PDF; unauthenticated access rejected. Local timings: 949 ms OFP, 31 ms Journey Log.
- `tests/smoke-weather.mjs`: exact EFHK summary **1,331 ms cold / 2 ms cached**; EFHV–EFHN briefing without winds **3,820 ms cold / 9 ms cached**, with nearby-report fallbacks labelled. Baseline route timing was approximately 3,798 ms cold / 8 ms cached: cold fallback latency is **not** claimed to have improved. The gains are direct-station access, duplicate suppression, controlled requests, cancellation and correctness. FMI returned 25 valid, non-future observations.
- `tests/qa-cockpit-ui.mjs`: local headless Chrome, synthetic flights, service worker blocked; **1194×834, 1024×768, 834×1194 and 1440×900**. No browser runtime errors. Checks include horizontal overflow, OFP action-bar clearance, alternate-input width, all six steps, Journey Log expand/Escape/Back, weather generation/PDF/reading/Back, real radar tiles and replay, manual-weather isolation, day mode and an emulated ±18 m device fix.
- Screenshots visually reviewed: OFP Overview/Route/Schedule/Fuel/Review, expanded Journey Log, weather reports/inbox/radar/PDF, Flights, aircraft, performance, documents, logbook and location details. Evidence is in the Codex task artifacts under `nov19-cockpit`, outside the source repository. Browser dimensions and emulated location are not physical iPad testing.

## Remaining limits

- No physical iPad Safari, installed-PWA, hardware Face ID/Touch ID, AirPrint or native iOS simulator validation was performed. No 120 FPS or zero-hitch guarantee is made.
- Radar coverage is the **Finland network**, not worldwide. The CAPPI product is not flight-level precipitation; transparent/no-measurement areas do not establish clear weather. The airport connection is a direct planning overview, not the filed waypoint route.
- Route weather compares airport reports and optionally existing winds data; continuous route forecasts, SIGMET/NOTAM aggregation and certified terrain/obstacle guidance are not implemented here. Partial decoding does not replace the full raw reports and official briefing sources.
- Browser geolocation accuracy is not HDOP or certified GPS integrity. Device position is not an approved navigation source.
- Session inbox history disappears on reload; no durable weather archive was added. Offline authentication and prepared offline flight packages remain separate work. The prior five-second splash and return-lock behavior are preserved.
- Existing aircraft performance restrictions and taxi-time assumptions are unchanged. Unsupported aircraft are not assigned invented performance values.
- No pilot records, documents, aircraft profiles or production database contents were deleted or rewritten by this upgrade. Test flights exist only in temporary local memory.

## Changed files

- Server/providers: `server.js`, new `lib/weather-service.js`, new `lib/fmi-radar.js`.
- OFP/documents: `views/operational-flight-plan.html`, new `public/pilot-ofp-layout.js`, `public/pilot-ofp-touch.css`, `public/pilot-ofp-checks.js`, `public/pilot-flight-documents.js`, `public/pilot-flight-documents.css`.
- Weather/UI: `views/booking-ops.html`, new `public/pilot-weather-workspace.js`, new `public/pilot-weather-workspace.css`, new `public/pilot-weather-radar.js`, `public/pilot-weather-viewer.js`.
- Map/position: `public/pilot-map-view.js`, `public/pilot-cockpit.js`, `public/pilot-airfields.js`.
- Cache/version: `public/pilot-sw.js`, `public/pilot-offline.html`, `views/pilot-login.html`.
- Tests: `tests/ofp-workflow.test.mjs`, `tests/pilot-map-view.test.mjs`, `tests/pilot-ofp-checks.test.mjs`, `tests/pilot-weather-reader.test.mjs`, `tests/pilot-workspaces.test.mjs`, new `tests/weather-service.test.mjs`, new `tests/smoke-weather.mjs`, new `tests/qa-cockpit-ui.mjs`.
- Report: this file. No source files or dependencies were deleted in this increment; obsolete inline radar and raw-weather-fetch code was replaced by the modules above.

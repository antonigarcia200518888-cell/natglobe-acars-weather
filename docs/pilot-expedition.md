# Pilot Ops — globe, reviewed imports and recorded flight replay

Release work: 18 November 2026. This is an incremental change to the existing Express/vanilla-JavaScript EFB, not a replacement application or a certified navigation system.

## Architecture inspected and preserved

- Express serves the existing authenticated Pilot Ops shell and embedded OFP editor. PostgreSQL JSONB remains the flight-record source of truth; the browser's existing `requests` / `selected` workflow still controls selection and unsaved-edit guards.
- MapLibre GL JS 5.24 renders the vector map/globe. The Leaflet and canvas fallbacks remain. Three.js and the existing representative airframe viewer remain; no unlicensed aircraft model was added.
- Existing access codes, passkeys, crew roles, movement records, controlled OFP PDFs, weather briefing reader/radar, flight documents, training metadata and reimbursement templates are preserved. The prior documentation/reimbursement implementation was already present, so it was not replaced with a second system.
- `PilotFlights` is a facade over existing state, not a new store. Planning surfaces use the saved OFP airport pair where available. Passenger reservation details are not rewritten by a planning change. Recorded tracks have a separate source and map layer.

## Delivered workflows

### Navigation and startup

The main dock is Map / Flights / OFP / Weather / Aircraft / Documents / More. Compact layouts keep the overflow menu. Performance and Logbook remain accessible from More and contextual shortcuts.

The login document presents the original full logo on white for approximately three seconds on the first unlock presentation in a browser session. Any keyboard/touch interaction can skip the wait. Authenticated requests go directly to the app; the splash never grants access. A successful unlock makes a full document navigation to Map, fixing the former hash-only redirect problem. Reloading an authenticated root entry opens Map without automatically selecting a saved flight.

The current-flight clear control unloads the workspace through the existing unsaved-OFP guard; it does not delete the flight. The selector includes category, planned/completed and imported filters, and searches aircraft/airport/date/status/source text. Imported recorded tracks remain discoverable even when attached to an NGA-created flight.

### Globe and airfields

- Pilot Ops starts in globe mode with black space, a restrained atmospheric rim, historical NASA Blue Marble imagery and separately configured elevation tiles.
- Real elevation uses Mapterhorn raster-DEM Terrarium tiles, 512 pixels, maximum zoom 12. Off / 1× / 1.5× controls distinguish actual elevation from exaggerated terrain. Source failures preserve the basemap and report unavailable elevation.
- Initial slow rotation stops on map interaction, flight planning, offscreen/hidden state and reduced-motion preferences. There is no 120 FPS guarantee.
- Airfields & globe allows catalog search, up to 50 explicit favorite ICAOs, airport information, departure/destination actions, and saving the current camera as the device's startup region. No arbitrary favorites are preselected.
- Map/Globe and day/night changes retain flight and recorded-track layers. This remains a geographic overview, not an aviation chart with certified airway/procedure decoding or obstacle clearance.

### Reviewed flight-plan imports

Flights → Import flight plan, or OFP Setup → Import / review route file:

1. Paste shared route text / ICAO FPL, or choose an uncompressed UTF-8 route file.
2. Review extracted fields, missing values, possible matches and warnings.
3. Explicitly choose a new or existing flight and its category.
4. Approve individual fields. Existing populated values start unchecked; corrections are identified as pilot-confirmed rather than imported.
5. Confirm, then continue in the existing OFP editor.

Supported inputs:

| Input | Behavior |
| --- | --- |
| Plain route / labeled shared text | Route, endpoint ICAOs and supported labeled planning fields; date/time and missing values require review |
| ICAO FPL text | Callsign, REG when supplied, airport pair, alternate, rules, level, route, EET and DOF-based UTC departure; mixed Y/Z rules require manual resolution |
| Garmin FlightPlan v1 `.fpl` | Resolves ordered route points against composite identifier/type/country references in the waypoint table |
| GPX `<rte>` | One compatible route; this is **not** a claim that ForeFlight exports GPX flight plans |

There is no ForeFlight login/API synchronization, flight filing, automatic subscription access, or transfer of proprietary performance calculations. Airway/procedure tokens are retained for review, not resolved to certified geometry. XML waypoint coordinates remain in the private source/processed record; the operational map displays an explicitly labeled endpoint overview.

Imported UTC is adapted once into the existing editor's local date/time and daylight-saving occurrence. The legacy `dateUtc` field actually stores the editor's local departure date; it was not destructively renamed. `scheduledOutIso` now exposes the computed UTC instant. Tests cover midnight rollover and both occurrences of the autumn clock change.

OFP Setup shows Import → Review → Complete missing inputs → Calculate → Preview → Generate PDF. Imported provenance and pilot corrections remain visible. Changed routes invalidate old route-dependent calculations/release state. Imported plans cannot be released before review. New imports do not invent fuel flow, cruise speed, reserve fuel or EET.

Only the existing verified OH-PMK profile is eligible for the aircraft-specific calculation/PDF path. Other registrations can be stored and reviewed but are explicitly blocked from using OH-PMK mass/balance and performance values. Matching an undashed registration to the verified profile currently requires a pilot correction to its stored spelling, `OH-PMK`.

### Recorded tracks and replay

Flights → Import track log supports:

- ForeFlight CSV headings: Timestamp (Unix seconds), Latitude, Longitude, optional Altitude (GPS/WGS84 feet), Speed (knots) and Course (true).
- GPX `<trk>/<trkseg>/<trkpt>` timestamps and elevations (metres converted to feet). Segment boundaries remain distinct.
- KML `gx:Track` aligned `when` / `gx:coord` arrays, and static `LineString` tracks. Non-absolute KML altitude is omitted rather than treated as terrain-independent altitude.

The review can preview without saving, explicitly associate a track with an existing flight, or create a categorized flight with pilot-confirmed airports/aircraft/planned time. Track endpoints are never silently assumed to be airports, a planned route, takeoff/landing times or a verified logbook entry.

Saved tracks and their original files are available under Flights → Saved tracks & sources. Replay provides play/pause, scrubber, 1×/5×/10×/30× speed, free/top-down/perspective camera, available altitude/speed readouts, and sampled SVG profiles. It starts paused. Position interpolation uses actual ordered timestamps and the short dateline crossing. Playback pauses across segments or gaps exceeding 120 seconds, with an explicit Skip recording gap action. Missing/non-increasing timing disables animation; absent altitude/speed stays absent. A directionless position dot replaces the aircraft glyph if course was not recorded.

The recorded track is separate from the planned route; a newly created track-only file has no invented planned route line. Recorded altitude is a readout, **not** a terrain-clearance calculation or altitude-positioned synthetic-vision model. Profiles are labeled by sampled point order and break at missing data/recording gaps.

A saved track can prepare an editable logbook draft using the existing prefill workflow. Its recorded range is added as a review note, not credited flight time. Existing entries are opened rather than overwritten; pilot verification/attestation remains required before saving a final entry.

## Persistence and security

Additive startup migration in `lib/aviation-import-routes.js`:

- `pilot_flight_imports`: UUID, flight foreign key, kind (`plan`/`track`), SHA-256, filename, original BYTEA, processed JSONB, UTC creation time.
- Unique `(flight_id, kind, sha256)` prevents identical repeated attachments. `(kind, sha256)` index supports duplicate lookup.
- Existing flight payload gains import provenance/review metadata and recorded-track count only as needed. No existing table or saved record is deleted or bulk-rewritten.
- Original and processed source writes share a transaction with flight updates. A source-specific advisory lock handles concurrent duplicate confirmation. Revision checks protect a stale review from overwriting later OFP/management changes; database-connection failures remove temporary in-memory ID reservations.
- Existing authenticated crew restrictions protect all import endpoints. Responses are private/no-store; original files download as octet-stream with `nosniff`. APIs, private files and external map tiles are excluded from the service-worker asset cache.
- Limits: 8 MiB per source, 12 sources per flight, 100,000 recorded positions, 80 route elements and the existing 240-character OFP route limit. XML DTD/entities, malformed XML, invalid coordinates/timestamps, compressed/binary uploads and over-limit inputs are rejected. Parsing uses bounded worker threads (two concurrent, 128 MiB each, 10-second deadline).
- PostgreSQL is required to confirm an import. Without it, review/preview remains available and saving reports unavailable storage. No fake saved state or local-only replacement database is introduced.

## Validation

- `node --test tests/*.test.mjs`: **113 passed, zero failed, zero skipped**, using the existing locally available PGlite runtime via `PILOT_TEST_PGLITE` for the two isolated SQL integration suites. No production database was used.
- Regression coverage includes access restrictions, original-byte retrieval, duplicate/stale revision rejection, rollback/connection failure, manual-field preservation, UTC/DST conversion, unsupported aircraft, malformed inputs, map/terrain fallback, favorites, replay gaps/dateline, splash/session handling and shared flight context.
- `node --check` for changed standalone JavaScript and VM parsing for inline EFB scripts. `git diff --check` passes. This repository has no build, lint or TypeScript-check script; those are not claimed as executed.
- Browser checks: 1180×820 and 1024×768 landscape, 820×1180 portrait, and 390×844 compact fallback. Checked globe, favorites, flight selection/clear, OFP summary/setup, route paste/review, protected existing values, unlock-to-Map and authenticated reload. No horizontal document overflow at the checked sizes; no console errors on the final fresh map session.
- The browser automation file chooser stalled. Therefore full browser file-upload/replay was **not** verified. Parsing, persistence and replay controls were validated with synthetic documented-format fixtures/API and DOM-harness tests. A real ForeFlight export and physical iPad/Safari pass remain important follow-up checks.
- Screenshots captured locally: `nga-globe-ipad-landscape.jpg`, `nga-globe-ipad-portrait.jpg`, `nga-ofp-ipad-landscape.jpg`, `nga-ofp-ipad-portrait.jpg`, `nga-import-review-landscape.jpg` in the Codex visualization artifact directory for this chat.

## Changed files

- Backend: `server.js`; new `lib/aviation-import.js`, `lib/aviation-import-worker.js`, `lib/aviation-import-routes.js`; pinned `fast-xml-parser` in `package.json` / `pnpm-lock.yaml`.
- Shell/flight workflow: `views/booking-ops.html`, `views/pilot-login.html`, `views/operational-flight-plan.html`, `public/pilot-app-shell.js`, `public/pilot-flight-deck.js`, `public/pilot-ofp-checks.js`.
- New modules: `public/pilot-flight-context.js`, `public/pilot-startup.js`, `public/pilot-airfields.js`, `public/pilot-imports.js`, `public/pilot-replay.js`, `public/pilot-expedition.css`.
- Renderer/runtime: `public/pilot-map-view.js`, `public/pilot-glass.js`, `public/pilot-touch-controls.js`, `public/pilot-sw.js`, `public/pilot-offline.html`.
- Tests: new `tests/aviation-import.test.mjs`, `tests/pilot-expedition.test.mjs`, `tests/fixtures/recorded-track.csv`; updated OFP, flight-deck and map-view suites.

## External references and remaining limits

- [ForeFlight track-log export formats](https://support.foreflight.com/hc/en-us/articles/27632618331927-How-can-a-Track-Log-be-exported), [CSV units](https://support.foreflight.com/hc/en-us/articles/214400218-What-format-or-units-are-displayed-within-the-Track-Log-CSV-file), [Unix timestamp format](https://support.foreflight.com/hc/en-us/articles/204039245-What-is-the-time-format-used-in-an-exported-CSV-Track-Log-file), [route sharing](https://support.foreflight.com/hc/en-us/articles/204981618-Is-it-possible-to-email-a-route-from-the-FPL-viewer-on-the-Maps-page).
- [Garmin FPL v1 schema](https://www8.garmin.com/xmlschemas/FlightPlanv1.xsd), [GPX 1.1](https://www.topografix.com/GPX/1/1/), [KML reference](https://developers.google.com/kml/documentation/kmlreference#gxtrack).
- [Mapterhorn data access](https://mapterhorn.com/data-access/) and [attribution](https://mapterhorn.com/attribution/). Existing OpenFreeMap and NASA GIBS attribution is retained in-map.
- No paid service, hosted Mac, native iOS migration, certified chart database, bulk offline map download, or new exact aircraft model was added. Tile availability/coverage and browser/WebGL performance remain external dependencies. Existing aircraft viewer is representative, not a newly verified model-specific asset.
- Existing weather sources, document permissions, reimbursement rules/calculations and native-wrapper experiments were preserved, not rewritten. Unified cross-document full-text search and certified airway/procedure reconstruction are not part of this delivered slice.

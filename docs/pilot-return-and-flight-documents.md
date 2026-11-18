# Pilot return lock and unified flight documents — increment 1

Date: 18 November 2026. Asset version: `2026-11-18-1`. Service-worker cache: v42.

This is the first increment of the comprehensive pilot-experience request, not a claim that the weather, radar, offline and full-app audits are complete.

## Approach

1. Remove the OFP viewer's navigation trap while preserving the existing flight record and PDF generator.
2. Place OFP and Journey Log in one reader with stable return controls and a common saved-data source.
3. Make actual movements explicit dated UTC records, with chronology validation.
4. Implement the requested five-second branded splash and fresh verification on foreground return, keeping the current editor in memory.
5. Simplify the dock, remove the unwanted aircraft model viewer, then run API, unit and browser checks before deployment.

## Bugs fixed / features delivered

- OFP preview previously fell back to navigating the top-level page to a raw PDF, removing the EFB return controls. It now renders inside a native HTML dialog with a fixed header: **Back to flight plan**, **Back to EFB**, normal view and Escape. No dependency on browser history or Fullscreen API support.
- Global SVG icon styling added a thick stroke to PDF preview text. Document text now explicitly excludes icon strokes. Zoom-out now actually reduces page width.
- The OFP and Journey Log share a single reader, including optional side-by-side display, zoom, regeneration, PDF downloads and print styling. SVG preview positions are captured from the same drawing pass that creates the PDF, rather than a second independently calculated layout.
- Journey Log body uses the existing embedded Courier New font. It includes saved flight identity, route, crew, dated actual movements, actual airborne/block durations and briefing notes. Long notes wrap and paginate. It is explicitly not a maintenance release or an automatically confirmed crew logbook entry.
- Manual movement controls now require a full UTC date and time. Invalid calendar dates and out-of-order movements are rejected server-side. Midnight is not inferred. Existing local-time API callers remain compatible; the new UI uses the explicit UTC path. Legacy clock-only records are retained and are not used to fabricate dated durations.
- Actual movements moved into **Flight schedule**; the separate Journey Log stage is gone. Seven larger, wrapping section controls remain. iPad movement cards use two columns rather than a three-plus-one arrangement; small screens stack them.
- A five-second white screen with the full NatGlobe Aviation logo appears on initial entry and foreground return. Internal section changes do not replay it. There is no tap-to-skip.
- The cockpit is covered immediately on backgrounding. On return it requires a fresh server-verified passkey or pilot-code login. Existing open flight/draft DOM is retained. Rapid return events share a single session-clear request, preventing a late logout race. Failed verification never reveals the workspace. A manual **Lock Pilot Ops** action is also available in the crew menu.
- An inactive pilot profile is now rejected during passkey verification.
- Dock: **Map | Flights | Plan | Weather | More**. Secondary workspaces remain available under More. Removed the unwanted 3D airframe viewer, its launch affordances, scripts and Three.js dependency. Aircraft profiles/status, the map globe and track replay remain intact. Deleted scripts are recoverable from Git history.

## Verification

- `node --test tests/*.test.mjs`: **122 passed, 0 failed, 0 skipped**, including disposable PostgreSQL-compatible integration tests through PGlite.
- `node --check server.js`: passed; inline scripts and new viewer/lock scripts also parse in the regression suite.
- There are no separate build, lint or TypeScript scripts in this vanilla HTML/JS + Express repository. No unsupported build result is claimed.
- `node tests/smoke-flight-documents.mjs`: isolated localhost server, `DATABASE_URL` unset, synthetic flight only. Verified invalid-date rejection, chronology rejection, four explicit overnight UTC movements, unauthenticated document access rejection, three OFP preview/PDF pages and one Journey Log page with a 55-minute block duration. Observed local generation: approximately **1,020 ms OFP / 29 ms Journey Log**. These are local document timings, not production or weather latency measurements.
- Browser checks: **1180×820** landscape, **820×1180** portrait, **1440×900** desktop. Visually reviewed document text, stable return controls, dock clearance, side-by-side comparison, portrait rendering and zoom-out. Verified Escape returns focus to Expand document. Verified an unsaved scratchpad note survives manual lock, white splash, code unlock, and reopening Notes. Wrong-code rejection and reload locking were also checked.
- Screenshot evidence saved outside the repository in the Codex task artifacts: `nga-return-splash.jpg`, `nga-ofp-landscape.jpg`, `nga-journey-portrait.jpg`, `nga-documents-desktop.jpg`, `nga-schedule-landscape.jpg`.
- Background/foreground, failed verification, another-profile rejection, rapid visibility events and offline failure are additionally covered by deterministic tests. Browser dimensions are not a substitute for physical iPad Safari testing. No hardware Face ID/Touch ID, AirPrint, physical printer, or iOS simulator run was performed.

## Authentication behavior and limits

- Register a device passkey from the existing **Crew → Device access** flow after code sign-in. The unlock screen provides **Use Face ID / Touch ID**; the browser requires a user gesture and the OS may offer its device PIN. A web app cannot force a specific biometric method.
- The user chose locking on every foreground return because installed web apps cannot reliably distinguish closing from app switching. OS authentication sheets are excluded while a verification is in progress.
- Unlock currently requires the server. Offline return fails closed with a reconnect/retry message; offline authentication and protected flight-package access require a separate design and are not simulated.
- This is a foreground privacy/session re-verification gate, not encryption of browser memory or secure erasure of prior downloads. A killed/reloaded browser process cannot preserve unsaved in-memory edits; save important work. Downloaded PDF files are outside this gate.
- Existing passkey/domain binding still applies. Register/use the passkey on the same domain used for the installed app.

## Files / boundaries

- New: `lib/flight-document-preview.js`, `lib/flight-movement-time.js`, `public/pilot-flight-documents.{js,css}`, `public/pilot-session-lock.{js,css}`, `tests/pilot-return-documents.test.mjs`, `tests/smoke-flight-documents.mjs`.
- Updated: server routes/PDF adapter; Pilot Ops, login and OFP views; startup, service worker and dock styles; existing navigation/workflow tests; dependency manifest/lockfile.
- Removed: `public/pilot-airframe.js`, `public/pilot-hangar.js`, Three.js dependency. No flight records, aircraft profiles, pilot documents or production database data were removed.

## Remaining phases — not delivered in this increment

- Weather/ACARS request audit, deduplication, cache/cancellation, request history and measured before/after latency.
- Radar/provider licensing and November 2026 FMI dataset migration verification; radar playback/freshness work.
- Prepared offline flight packages and usable offline verification; real backend/provider health monitoring.
- Wider long-content, error-state and modal audit across all EFB modules, plus physical iPad Safari/installed-PWA validation.
- Further OFP calculation ergonomics and shared readiness/airport workflows. Existing verified-aircraft restrictions remain; no performance values were invented for unsupported aircraft.

No paid services or macOS CI minutes were used.

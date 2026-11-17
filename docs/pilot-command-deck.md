# Pilot Ops command deck

The production interface remains the authenticated Express/vanilla-JavaScript application at `/booking-ops`. MapLibre and Leaflet still own their maps. The active-flight selection, OFP, logbook, existing documents and passenger workflows have not been migrated.

## Storage and permissions

- The existing flight JSON gains an optional `management` field. A pilot-created file has an explicit category. Historical flights remain unclassified under **All files** until a pilot chooses one. Reclassifying a training flight retains its instructor, exercises and notes; actual times are read from the OFP, never inferred.
- The separate `pilot_flight_documents` Postgres table stores at most ten PDF/JPEG/PNG files, up to 2 MiB per file, per flight. Upload, list, read and delete require flight-crew access; document IDs are scoped to a flight. Files are not stored in the service-worker cache. A missing document store is an explicit error, not an ephemeral successful upload.
- Private files link to the existing reimbursement editor, charge rows, preview, split and PDF generator. Editing financial records retains the existing operations authorization. The template is not a determination of expense eligibility.

## Presentation and data limits

- The original wing and wordmark supply the navy, silver and paper colors. Glass belongs to the dock and floating controls, not raw reports. Phone overflow exposes the existing command menu. Browser Back and the standalone weather reader remain independent.
- The map uses OpenFreeMap geography. Political boundaries are not controlled airspace. There is no new certified airspace or terrain dataset.
- The Three.js airframe is original representative geometry, not a model of OH-PMK. It is loaded only when opened, renders on camera/size changes and releases its GPU resources when closed. Without WebGL the fleet photo remains available.
- METAR/TAF reports are from NOAA Aviation Weather Center. Comparison views label source stations, nearby reports, UTC age at retrieval, and selected forecast groups. The raw TAF remains authoritative; the partial decode does not interpret every token. Winds are GFS model pressure levels from Open-Meteo. Weather PDF preview requests a fresh export. See [AWC flight categories](https://aviationweather.gov/gfa/help/) for ceiling and visibility thresholds.

## Verification

Run `node --test tests/*.test.mjs`. To exercise uploads and persistence against a **disposable** embedded PostgreSQL database, set `PILOT_TEST_PGLITE` to the module URL of a local PGlite installation before running the suite. This optional test dependency is not installed on Render. The application is served as ES modules/static assets; it has no compilation, lint, or TypeScript build script.

Verified for this release: 97 passing tests with the database integration enabled, inline-script and server syntax checks, frozen dependency-lock validation, and whitespace checks. Browser checks covered 1194×834 landscape, 834×1194 portrait, 390×844 phone and 1440×900 desktop. These were browser viewport tests, not physical-iPad performance measurements. Tested interactions included Training flight creation and notes, day/night switching, aircraft rotation, live weather generation and PDF return. Private statement saving and authenticated PDF output were also exercised with a temporary local flight. No test flight was created on production.

No licensed exact aircraft model, controlled-airspace feed, route-wide SIGMET/AIRMET aggregation or historical weather store was found. Those features are not represented as operational in this release. Existing radar and runway-performance tools remain available.

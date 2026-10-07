# NGA EFB design preview

This is a working HTML/CSS design example, separate from the production Pilot Ops workflow. It does not authenticate, read pilot records, save flight plans, acknowledge operational checks, or release flights. Sample data is prominently identified. No Jeppesen / ForeFlight charts, services or affiliation are included.

## Open

Run the existing application, then visit `/efb-preview/`. Assets are served by its existing static middleware. The preview is not linked into the operational navigation.

## Modules

- `index.html`: semantic status bar, five tab panels, full-workspace map with a floating flight panel, forms and accessible empty states.
- `efb-dashboard.css`: theme tokens, responsive layout, glass control surfaces and solid data cards. Blur is limited to the flight-panel frame, map toolbars and bottom dock. Subtle rims and shadows separate these controls from the map.
- `efb-dashboard.js`: navigation, themes, glass/solid preference, flight-panel/map layout, local sample-plan calculation, demonstration checklist, device UTC, optional battery reporting and one map instance.

## Theme and layout contract

```html
<html data-theme="night" data-material="glass">
  <div class="efb-app">
    <header class="status-bar" aria-label="Device and flight status">…</header>
    <main class="main-content">
      <div class="workspace-bar">…</div>
      <section class="map-workspace" data-layout="split">
        <aside class="flight-pane glass-surface">…</aside>
        <section class="map-pane">…</section>
      </section>
    </main>
    <nav class="bottom-nav glass-surface" role="tablist" aria-label="EFB workspaces">…</nav>
  </div>
</html>
```

Set `data-theme` to `day` or `night` and `data-material` to `glass` or `solid`. Set `data-layout` on the map workspace to `split` (flight panel shown) or `map`. The controller synchronizes these attributes with `aria-pressed` buttons. Preferences use an isolated `nga-efb-design-preview-v1` local-storage key; sample flight/checklist data is not persisted. The system's reduced-transparency and increased-contrast preferences override glass. Browsers without backdrop-filter support use opaque surfaces automatically.

On tablets the flight panel floats above a full-workspace map. Phones use a compact flight sheet above the bottom dock. Route fitting accounts for the visible sheet and map controls, including after rotation; zooming and panning remain manual between layout changes. Reading panes reserve space for the floating dock so their last controls are reachable. The application uses dynamic viewport height and safe-area insets. All action buttons are at least 48 × 48 CSS pixels. Main figures are 23–38px; native system sans-serif fonts are used for controls, with Courier New limited to route identifiers. Validate legibility on the actual mounted iPad at the intended viewing distance.

## Honest data states

- UTC is the device clock, not synchronized dispatch time.
- Online/offline is the browser's connectivity hint, not an API-health or data-currency guarantee.
- GPS always says not connected. This example does not request location permission or display an invented accuracy/ownship position.
- Battery is read from the browser API when available; unsupported browsers display N/A, not an estimated percentage.
- Plates show an unconnected-library state. Current-cycle, georeferencing and chart controls require an authorized source.
- Sample planning uses direct geographic distance and a still-air time estimate. Flight rules and altitude are not validated operationally.
- The checklist contains UI testing tasks, never aircraft procedures or a PIC release.

## Map adapter

This example reuses the project's local Leaflet 1.9.4 assets and displays standard OpenStreetMap tiles. [OpenStreetMap's tile policy](https://operations.osmfoundation.org/policies/tiles/) requires visible attribution, normal browser caching and no bulk/offline prefetching. The preview retains attribution and browser referrers, loads only the visible map, and has no tile service worker. Tile failures are shown explicitly. Before production use, choose a provider with suitable licensing, capacity and service guarantees.

Night filtering affects the geographic basemap only, not route overlays or the UI. It is not intended for applying to approved aviation charts, weather colors, terrain depiction or hazard layers. Replace the basemap adapter with an authorized chart provider when integrating; preserve the provider's symbology and data-validity requirements.

## Integration boundaries

Keep the current authenticated selected-flight model and all server-side aircraft limits / release rules. Replace sample plan data with that model; do not treat the preview's calculated numbers or green UI states as release authority. Add source time, validity, loading, unavailable and stale states for each connected data product. Treat these colors as conventional warning/caution/normal semantics, not a claim of certification or a universal aviation standard.

No packages or build tooling are added. The clock stops its interval when the page is hidden; one ResizeObserver batches map resize events. Production authentication, maps, OFP and service-worker caches are unchanged.

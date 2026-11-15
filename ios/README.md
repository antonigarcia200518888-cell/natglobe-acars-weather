# Pilot Ops: flight switcher migration

The EFB at `/booking-ops` is a web app. It does not acquire native SwiftUI
materials when installed on the Home Screen. This slice keeps the existing EFB
and adds one optional native presentation, not a new flight-planning engine.

| Existing surface | Treatment |
| --- | --- |
| Workspace title, flight-selector chip and status toolbar | Keep in the web header; size from content, wrap when necessary, show the route instead of repeating the app name. Brushed aluminum is trim, not a background for operational text. |
| Flight-picker dialog, close button, create button, filter chips | A native host presents one system sheet, searchable list, segmented picker and paired glass actions. The web dialog remains available to Safari, the PWA and failed native presentations. |
| Flight rows, active state, search results, map, OFP fields and unsaved-edit dialog | Plain content. No layered blur, interactive glass on labels, custom sheet backdrop, or glass-effect morph between unrelated elements. |

**Integration.** Add the local Swift package `FlightSwitcher` to an iOS 17+
target using Xcode 26. Install a retained `FlightPickerBridge` on its existing
WKWebView with the exact first-party HTTPS `/booking-ops` URL. Attach
`.pilotFlightSwitcher(bridge)` to the persistent SwiftUI host and uninstall on
teardown. The bridge only accepts its own web view's top-level page, passes
display metadata without manifests or payment data, and replies after
dismissal. The page re-resolves the ID against its current queue and calls its
existing guarded activation or creation action. No duplicate database/session.

**Materials.** On iOS 26 the close/create controls use system glass button
styles grouped in a single GlassEffectContainer. They already respond to touch:
there is no extra blur or interactive overlay. The sheet transition is native,
so no custom namespace or morph is necessary. On iOS 17–18, with Reduce
Transparency, or with the EFB's solid preference, the action deck uses bordered
buttons on opaque system background. Search/list/scope and save behavior are
the same. Large accessibility type uses vertical actions and inline scopes.

**Simulator validation** (the host is test-only and does not sign in to Render):

1. Generate the `PilotOpsValidation` scheme with `xcodegen generate --spec ios/Validation/project.yml`.
2. Start `node ios/Validation/fixture-server.mjs`. It serves only loopback with two synthetic flights and the real flight-deck adapter.
3. In XcodeBuildMCP, select `ios/Validation/PilotOpsValidation.xcodeproj`, scheme `PilotOpsValidation`, and an iPad Air 11-inch (M3) running iPadOS 26.2. Build and run. The GitHub job uses the equivalent XcodeBuildMCP CLI.
4. Run the scheme's UI tests. Open the flight switcher, capture the populated sheet, filter Today, select DEMO-01, cancel without changing the flight, and create. The native screenshot is an XCTest attachment. The public-repo workflow exports a small JPEG into the standard job log without an artifact-storage upload. Run the same scheme on iPadOS 18 separately to verify the older OS visually.

Design references: [Build iOS Apps / SwiftUI Liquid Glass](https://github.com/openai/plugins/tree/main/plugins/build-ios-apps/skills/swiftui-liquid-glass) and [Apple's Liquid Glass guidance](https://developer.apple.com/documentation/swiftui/applying-liquid-glass-to-custom-views).

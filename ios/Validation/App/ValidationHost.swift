import SwiftUI
import WebKit
import PilotFlightSwitcher

@main struct ValidationHost: App {
    @StateObject private var bridge = FlightPickerBridge()
    var body: some Scene {
        WindowGroup { LoopbackPreview(bridge: bridge).pilotFlightSwitcher(bridge) }
    }
}

private struct LoopbackPreview: UIViewRepresentable {
    let bridge: FlightPickerBridge
    func makeCoordinator() -> FlightPickerBridge { bridge }
    func makeUIView(context: Context) -> WKWebView {
        let config = WKWebViewConfiguration()
        config.websiteDataStore = .nonPersistent()
        let view = WKWebView(frame: .zero, configuration: config)
        // Simulator-only host: never connect fixtures to the deployed EFB.
        let url = URL(string: "http://localhost:4179/booking-ops")!
        bridge.install(on: view, pilotOpsURL: url)
        view.load(URLRequest(url: url))
        return view
    }
    func updateUIView(_ view: WKWebView, context: Context) {}
    static func dismantleUIView(_ view: WKWebView, coordinator: FlightPickerBridge) { coordinator.uninstall() }
}

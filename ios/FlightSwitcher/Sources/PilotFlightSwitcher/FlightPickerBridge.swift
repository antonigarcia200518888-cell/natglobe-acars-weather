import SwiftUI
import WebKit

/// Install once on the host's web view. No navigation delegate, cookies, or
/// backend requests are replaced. The web page still owns flight activation.
@MainActor public final class FlightPickerBridge: NSObject, ObservableObject, WKScriptMessageHandlerWithReply {
    struct Presentation: Identifiable {
        let id = UUID()
        let request: FlightPickerRequest
    }
    @Published fileprivate var presentation: Presentation?
    private var reply: (@MainActor @Sendable (Any?, String?) -> Void)?
    private var action = FlightPickerAction.cancel
    private weak var webView: WKWebView?
    private var allowedURL: URL?

    public func install(on view: WKWebView, pilotOpsURL: URL) {
        uninstall()
        webView = view
        allowedURL = pilotOpsURL
        view.configuration.userContentController.addScriptMessageHandler(self, contentWorld: .page, name: "ngaFlightPicker")
    }

    public func uninstall() {
        webView?.configuration.userContentController.removeScriptMessageHandler(forName: "ngaFlightPicker", contentWorld: .page)
        presentation = nil
        action = .cancel
        complete()
        webView = nil
        allowedURL = nil
    }

    public func userContentController(_ controller: WKUserContentController, didReceive message: WKScriptMessage,
                                     replyHandler: @escaping @MainActor @Sendable (Any?, String?) -> Void) {
        guard message.name == "ngaFlightPicker", message.frameInfo.isMainFrame, message.webView === webView,
              let allowedURL, let url = message.frameInfo.request.url,
              url.scheme == allowedURL.scheme, url.host == allowedURL.host, url.port == allowedURL.port,
              url.path == "/booking-ops", url.path == allowedURL.path,
              url.scheme == "https" || (url.scheme == "http" && ["localhost","127.0.0.1"].contains(url.host ?? "")),
              reply == nil, JSONSerialization.isValidJSONObject(message.body),
              let data = try? JSONSerialization.data(withJSONObject: message.body), data.count <= 2_000_000,
              let request = try? JSONDecoder().decode(FlightPickerRequest.self, from: data),
              request.version == 1, Set(request.flights.map(\.id)).count == request.flights.count
        else { replyHandler(nil, "Flight picker unavailable"); return }
        reply = replyHandler
        action = .cancel
        presentation = Presentation(request: request)
    }

    fileprivate func choose(_ value: FlightPickerAction) {
        guard presentation != nil else { return }
        action = value
        presentation = nil
    }

    fileprivate func complete() {
        let send = reply
        reply = nil
        switch action {
        case .cancel: send?(["action":"cancel"], nil)
        case .create: send?(["action":"create"], nil)
        case .select(let id): send?(["action":"select", "id":id], nil)
        }
    }
}

private struct FlightPickerPresenter: ViewModifier {
    @ObservedObject var bridge: FlightPickerBridge
    func body(content: Content) -> some View {
        content.sheet(item: $bridge.presentation, onDismiss: bridge.complete) { item in
            FlightSwitcherSheet(request: item.request, onAction: bridge.choose)
                .presentationDragIndicator(.visible)
        }
    }
}

public extension View {
    /// Attach to the persistent web-view host, not to a changing toolbar item.
    func pilotFlightSwitcher(_ bridge: FlightPickerBridge) -> some View {
        modifier(FlightPickerPresenter(bridge: bridge))
    }
}

import SwiftUI

public struct FlightPickerRequest: Decodable {
    public struct Flight: Decodable, Identifiable {
        public let id, dep, arr, date, time, aircraft, status, search: String
        public let pilotFile: Bool

        fileprivate var searchable: String {
            [id, dep, arr, date, time, aircraft, search].joined(separator: " ")
        }
    }
    public let version: Int
    public let selected: String?
    public let today: String // Company's operational date supplied by Pilot Ops.
    public let flights: [Flight]
    public let night, solid: Bool?
}

public enum FlightPickerAction {
    case select(String), create, cancel
}

@MainActor public struct FlightSwitcherSheet: View {
    private enum Scope: String, CaseIterable { case all = "All flights", today = "Today", manual = "Pilot files" }
    private let request: FlightPickerRequest
    private let finish: (FlightPickerAction) -> Void
    @State private var query = ""
    @State private var scope = Scope.all
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency
    @Environment(\.dynamicTypeSize) private var typeSize

    public init(request: FlightPickerRequest, onAction: @escaping (FlightPickerAction) -> Void) {
        self.request = request
        self.finish = onAction
    }

    private var matches: [FlightPickerRequest.Flight] {
        let words = query.split(whereSeparator: \.isWhitespace).map(String.init)
        return request.flights.filter { flight in
            (scope != .today || flight.date == request.today) &&
            (scope != .manual || flight.pilotFile) &&
            words.allSatisfy { flight.searchable.localizedCaseInsensitiveContains($0) }
        }
    }

    public var body: some View {
        let rows = matches
        NavigationStack {
            List {
                Section {
                    if typeSize.isAccessibilitySize { scopes.pickerStyle(.inline) }
                    else { scopes.pickerStyle(.segmented) }
                }
                Section("\(rows.count) flights") {
                    if rows.isEmpty {
                        ContentUnavailableView("No matching flights", systemImage: "airplane",
                            description: Text("Change the search or filter, or create a pilot flight."))
                    }
                    ForEach(rows) { flight in
                        Button { finish(.select(flight.id)) } label: {
                            VStack(alignment: .leading, spacing: 5) {
                                Text("\(flight.dep) → \(flight.arr)").font(.headline.monospaced())
                                Text([flight.date, flight.time.isEmpty ? "" : "\(flight.time)L", flight.aircraft]
                                    .filter { !$0.isEmpty }.joined(separator: " · ")).font(.subheadline)
                                Text([flight.id, flight.pilotFile ? "Pilot file" : flight.status]
                                    .filter { !$0.isEmpty }.joined(separator: " · ")).font(.caption).foregroundStyle(.secondary)
                                if flight.id == request.selected {
                                    Label("Active flight", systemImage: "checkmark.circle").font(.caption.bold())
                                }
                            }
                            .frame(maxWidth: .infinity, minHeight: 48, alignment: .leading)
                            .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)
                        .accessibilityIdentifier("flight-\(flight.id)")
                    }
                }
            }
            .navigationTitle("Your flights")
            .searchable(text: $query, prompt: "Airport, date, aircraft or reference")
            .safeAreaInset(edge: .bottom) { actionDeck.padding(.horizontal).padding(.vertical, 8) }
        }
        .preferredColorScheme(request.night.map { $0 ? .dark : .light })
    }

    private var scopes: some View {
        Picker("Flight list", selection: $scope) {
            ForEach(Scope.allCases, id: \.self) { Text($0.rawValue).tag($0) }
        }
    }
    private var closeButton: some View {
        Button { finish(.cancel) } label: { Text("Close").frame(minHeight: 40) }
    }
    private var createButton: some View {
        Button { finish(.create) } label: { Label("Create pilot flight", systemImage: "plus").frame(minHeight: 40) }
    }
    private var actionLayout: AnyLayout {
        typeSize.isAccessibilitySize ? AnyLayout(VStackLayout(spacing: 12)) : AnyLayout(HStackLayout(spacing: 16))
    }
    @ViewBuilder private var actionDeck: some View {
        if #available(iOS 26, *), !reduceTransparency, request.solid != true {
            GlassEffectContainer(spacing: 8) {
                actionLayout {
                    closeButton.buttonStyle(.glass)
                    createButton.buttonStyle(.glassProminent)
                }
            }.buttonBorderShape(.capsule)
        } else {
            actionLayout {
                closeButton.buttonStyle(.bordered)
                createButton.buttonStyle(.borderedProminent)
            }
            .buttonBorderShape(.capsule)
            .padding(6)
            .background(Color(uiColor: .systemBackground), in: RoundedRectangle(cornerRadius: 18))
        }
    }
}

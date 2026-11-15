// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "PilotFlightSwitcher",
    platforms: [.iOS(.v17)],
    products: [.library(name: "PilotFlightSwitcher", targets: ["PilotFlightSwitcher"])],
    targets: [.target(name: "PilotFlightSwitcher")]
)

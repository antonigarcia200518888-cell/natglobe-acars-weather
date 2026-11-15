import XCTest

final class FlightSwitcherChecks: XCTestCase {
    @MainActor func testSelectCancelAndCreate() throws {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launch()
        let open = app.webViews.buttons["Select flight"]
        XCTAssertTrue(open.waitForExistence(timeout: 30))
        open.tap()
        let first = app.buttons["flight-DEMO-01"]
        XCTAssertTrue(first.waitForExistence(timeout: 10))
        XCTAssertTrue(app.buttons["flight-DEMO-02"].exists)
        let attachment = XCTAttachment(screenshot: app.screenshot())
        attachment.name = "Native-flight-switcher"
        attachment.lifetime = .keepAlways
        add(attachment)
        app.buttons["Today"].tap()
        XCTAssertFalse(app.buttons["flight-DEMO-02"].exists)
        first.tap()
        XCTAssertTrue(app.webViews.staticTexts["Active: DEMO-01"].waitForExistence(timeout: 10))
        open.tap()
        app.buttons["Close"].tap()
        XCTAssertTrue(open.waitForExistence(timeout: 10))
        XCTAssertTrue(app.webViews.staticTexts["Active: DEMO-01"].exists)
        open.tap()
        app.buttons["Create pilot flight"].tap()
        XCTAssertTrue(app.webViews.staticTexts["Create requested"].waitForExistence(timeout: 10))
    }
}

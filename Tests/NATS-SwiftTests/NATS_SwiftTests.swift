import XCTest
@testable import NATS_Swift

final class NATS_SwiftTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(NATS_Swift().text, "Hello, World!")
    }


    static var allTests = [
        ("testExample", testExample),
    ]
}

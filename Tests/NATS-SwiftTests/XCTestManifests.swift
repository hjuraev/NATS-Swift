import XCTest

#if !os(macOS)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(NATS_SwiftTests.allTests),
    ]
}
#endif
import XCTest

import NATS_SwiftTests

var tests = [XCTestCaseEntry]()
tests += NATS_SwiftTests.allTests()
XCTMain(tests)
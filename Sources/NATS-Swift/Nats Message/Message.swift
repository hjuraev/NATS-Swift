//
//  Message.swift
//  Run
//
//  Created by Halimjon Juraev on 2/2/18.
//

import Foundation

public struct NatsMessage {
    public let headers: Headers
    public let payload: Data
    public let sub: NatsSubscription?
    
    public struct Headers: Codable {
        public let subject: String
        public let sid: String
        public let replay: String?
        public let size: Int
    }
}
public struct NatsError {
    public let description: String?
}



final class PartialNatsMessage {
    public var rawValue: Data = Data()
    public var proto: Proto?
    public var headers: NatsMessage.Headers?
    public var payload: Data?
    public var state: NatsParser.MESSAGE_STATE = .OP_START
    public var info: Data?
    public var consumed: Int = 0
    public var completed: Bool = false
}



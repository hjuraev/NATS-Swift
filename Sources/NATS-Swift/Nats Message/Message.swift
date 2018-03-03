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
        let subject: String
        let sid: String
        let replay: String?
        let size: Int
    }
}
public struct NatsError {
    let description: String?
}



final class PartialNatsMessage {
    var rawValue: Data = Data()
    var proto: Proto?
    var headers: NatsMessage.Headers?
    var payload: Data?
    var state: NatsParser.MESSAGE_STATE = .OP_START
    var info: Data?
    var consumed: Int = 0
    var completed: Bool = false
}



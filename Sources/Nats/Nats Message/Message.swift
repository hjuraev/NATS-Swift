//
//  Message.swift
//  Run
//
//  Created by Halimjon Juraev on 2/2/18.
//

import Foundation
import Vapor

public struct MSG {
    public let headers: Headers
    public let payload: Data
    public let sub: NatsSubscription?
    public var container: Container?
    
    public struct Headers: Codable {
        public let subject: String
        public let sid: UUID
        public let replay: String?
        public let size: Int
    }
    
}


enum NatsMessage {
    case MSG(MSG)
    case INFO(Server)
    case OK
    case ERR(NatsError)
    case PONG
    case PING
}


public struct NatsError {
    public let description: String?
}


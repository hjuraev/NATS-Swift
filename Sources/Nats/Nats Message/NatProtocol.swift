//
//  NatsProtocol.swift
//  Run
//
//  Created by Halimjon Juraev on 12/29/17.
//

import Vapor
import Foundation
import NIO

public struct NatsRequest {
    public let id: UUID
    public let subject: String
    public let promise: EventLoopPromise<MSG>
    public let scheduler: Scheduled<()>?
}

public struct NatsSubscription {
    public let id: UUID
    public let subject: String
    public let queueGroup: String
    fileprivate(set) var count: UInt
    public var callback: ((MSG) -> ())?
    public func sub() -> String {
        let group: () -> String = {
            if self.queueGroup.count > 0 {
                return "\(self.queueGroup) "
            }
            return self.queueGroup
        }
        
        return "\(Proto.SUB.rawValue) \(subject) \(group())\(id.uuidString)\r\n"
    }
    
    public func unsub(_ max: UInt32) -> String {
        let wait: () -> String = {
            if max > 0 {
                return " \(max)"
            }
            return ""
        }
        return "\(Proto.UNSUB.rawValue) \(id)\(wait)\r\n"
    }
    
    mutating func counter() {
        self.count += 1
    }
}


public enum Proto: String, Codable {
    case CONNECT = "CONNECT"
    case SUB = "SUB"
    case UNSUB = "UNSUB"
    case PUB = "PUB"
    case MSG = "MSG"
    case INFO = "INFO"
    case OK = "+OK"
    case ERR = "-ERR"
    case PONG = "PONG"
    case PING = "PING"
}

public struct Server: Codable {
    public let server_id: String
    public let version: String
    public let go: String
    public let host: String
    public let port: Int
    public let auth_required: Bool
    public let ssl_required: Bool
    public let tls_required: Bool
    public let tls_verify: Bool
    public let max_payload: Int
    public let connect_urls: [String]
}


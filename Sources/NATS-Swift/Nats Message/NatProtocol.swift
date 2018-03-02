//
//  NatsProtocol.swift
//  Run
//
//  Created by Halimjon Juraev on 12/29/17.
//

import Vapor
import Foundation

public struct NatsSubscription {
    public let id: UUID
    public let subject: String
    public let queueGroup: String
    fileprivate(set) var count: UInt
    
    func sub() -> String {
        let group: () -> String = {
            if self.queueGroup.count > 0 {
                return "\(self.queueGroup) "
            }
            return self.queueGroup
        }
        
        return "\(Proto.SUB.rawValue) \(subject) \(group())\(id.uuidString)\r\n"
    }
    
    func unsub(_ max: UInt32) -> String {
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


enum Proto: String, Codable {
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

struct Server: Codable {
    let server_id: String
    let version: String
    let go: String
    let host: String
    let port: Int
    let auth_required: Bool
    let ssl_required: Bool
    let tls_required: Bool
    let tls_verify: Bool
    let max_payload: Int
    let connect_urls: [String]

}


//
//  NatsProtocol.swift
//  Run
//
//  Created by Halimjon Juraev on 12/29/17.
//

import Vapor
import Foundation
import NIO
import SwiftProtobuf

public enum retunValue {
    case MSG(((NatsMessage) -> ()))
    case REQ(NatsRequest)
    // STREAMING
    case pubAck(PubMSG)
    case StreamingMSG(StreamingMSG)
}

public class StreamingMSG {
    public let callback: (((NatsMessage) -> ()))
    public let ackInbox: String
    
    init(callback: @escaping (((NatsMessage) -> ())), ackInbox: String) {
        self.callback = callback
        self.ackInbox = ackInbox
    }
}

public class PubMSG {
    let pubMSG: Pb_PubMsg
    let uuid: UUID
    let task: RepeatedTask
    
    init(pubMSG: Pb_PubMsg, uuid: UUID, task: RepeatedTask) {
        self.pubMSG = pubMSG
        self.uuid = uuid
        self.task = task
    }
}

public class NatsRequest {
    
    public let promise: EventLoopPromise<NatsMessage>
    public let scheduler: Scheduled<()>?
    public let numberOfResponse: NumberOfResponse
    init(promise: EventLoopPromise<NatsMessage>, scheduler: Scheduled<()>?, numberOfResponse: NumberOfResponse){
        self.promise = promise
        self.scheduler = scheduler
        self.numberOfResponse = numberOfResponse
    }
    
    public enum NumberOfResponse {
        case single
        case multiple(MultipleResponses)
        case unlimited
    }
    
    public class MultipleResponses {
        let count: Int
        private(set) var received: Int = 0
        
        init(count: Int) {
            self.count = count
        }
        
        func counter() -> Bool {
            if count > received {
                received = received+1
                return true
            } else {
                return false
            }
        }
    }
    
    
}


public class NatsCallbacks {
    public let id: UUID
    public let subject: String
    public let queueGroup: String
    fileprivate(set) var count: UInt = 0
    public var callback: retunValue
    
    init(id: UUID, subject: String, queueGroup: String, callback: retunValue) {
        self.id = id
        self.subject = subject
        self.queueGroup = queueGroup
        self.callback = callback
    }
    
    
    public func sub() -> Data {
        let group: () -> String = {
            if self.queueGroup.count > 0 {
                return "\(self.queueGroup) "
            }
            return self.queueGroup
        }
        
        return "\(Proto.SUB.rawValue) \(subject) \(group())\(id.uuidString)\r\n".data(using: .utf8) ?? Data()
    }
    
    public func unsub(_ max: UInt32) -> Data {
        let wait: () -> String = {
            if max > 0 {
                return " \(max)"
            }
            return ""
        }
        return "\(Proto.UNSUB.rawValue) \(id)\(wait())\r\n".data(using: .utf8) ?? Data()
    }
    
    func counter() {
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
    public let server_id: String?
    public let version: String?
    public let go: String?
    public let host: String?
    public let port: Int?
    public let auth_required: Bool?
    public let ssl_required: Bool?
    public let tls_required: Bool?
    public let tls_verify: Bool?
    public let max_payload: Int?
    public let connect_urls: [String]?
}


public struct NatsGeneralError: Debuggable {
    /// See `Debuggable`.
    public static let readableName = "Vapor Error"
    
    /// See `Debuggable`.
    public let identifier: String
    
    /// See `Debuggable`.
    public var reason: String
    
    /// See `Debuggable`.
    public var sourceLocation: SourceLocation?
    
    /// See `Debuggable`.
    public var stackTrace: [String]
    
    /// See `Debuggable`.
    public var suggestedFixes: [String]
    
    /// See `Debuggable`.
    public var possibleCauses: [String]
    
    /// Creates a new `VaporError`.
    public init(
        identifier: String,
        reason: String,
        suggestedFixes: [String] = [],
        possibleCauses: [String] = [],
        file: String = #file,
        function: String = #function,
        line: UInt = #line,
        column: UInt = #column
        ) {
        self.identifier = identifier
        self.reason = reason
        self.sourceLocation = SourceLocation(file: file, function: function, line: line, column: column, range: nil)
        self.stackTrace = VaporError.makeStackTrace()
        self.suggestedFixes = suggestedFixes
        self.possibleCauses = possibleCauses
    }
}

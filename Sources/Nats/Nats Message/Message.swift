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
    
    public struct Headers: Codable {
        public let subject: String
        public let sid: UUID
        public let reply: String?
        public let size: Int
    }
    
}

public protocol NatsResponder  {
    func publish(_ subject: String, payload: String) throws -> EventLoopFuture<Void>
    func request(_ subject: String, payload: String, timeout: Int) throws -> EventLoopFuture<NatsMessage>
    func unsubscribe()
}

public final class NatsMessage: ContainerAlias, DatabaseConnectable,  CustomStringConvertible, CustomDebugStringConvertible, NatsResponder, Logger  {
    
    
    public func log(_ string: String, at level: LogLevel, file: String, function: String, line: UInt, column: UInt) {
        let text: String = "[ \(level) ] \(string)  --  (\(file):\(line))"
        debugPrint(text)
    }
    
    public func publish(_ subject: String, payload: String) throws -> EventLoopFuture<Void> {
        let pub: () -> Data = {
            if let data = payload.data(using: String.Encoding.utf8) {
                return "\(Proto.PUB.rawValue) \(subject) \(data.count)\r\n\(payload)\r\n".data(using: .utf8) ?? Data()
            }
            return Data()
        }
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        return handler.write(ctx: ctx, data: pub())
    }
    
    public func request(_ subject: String, payload: String, timeout: Int) throws -> EventLoopFuture<NatsMessage> {
        let uuid = UUID()
        
        let promise = ctx.eventLoop.newPromise(NatsMessage.self)
        
        let storage = try sharedContainer.make(NatsResponseStorage.self)
        
        let schedule = ctx.eventLoop.scheduleTask(in: .seconds(timeout), {
            storage.requestsStorage.removeValue(forKey: uuid)
            let error = NatsGeneralError(identifier: "NATS TIMEOUT", reason: "TIMEOUT SUBJECT: \(subject)")
            promise.fail(error: error)
            return
        })
        
        let sub = "\(Proto.SUB.rawValue) \(uuid.uuidString) \(uuid.uuidString)\r\n\(Proto.UNSUB.rawValue) \(uuid.uuidString) \(1)\r\n".data(using: .utf8)!
        
        
        
        let request = NatsRequest(id: uuid, subject: subject, promise: promise, scheduler: schedule)
        
        storage.requestsStorage.updateValue(request, forKey: uuid)
        let requestSocket: () -> Data = {
            if let data = payload.data(using: String.Encoding.utf8) {
                return "\(Proto.PUB.rawValue) \(subject) \(uuid.uuidString) \(data.count)\r\n\(payload)\r\n".data(using: .utf8) ?? Data()
            }
            return Data()
        }
        
        let finalData = sub + requestSocket()
        
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        handler.write(ctx: ctx, data: finalData).catch { error in
            debugPrint(error)
        }
        return promise.futureResult
    }
    
    public func unsubscribe() {
        // TODO: IMPLEMENT ME
    }
    
    
    
    
    public static var aliasedContainer: KeyPath<NatsMessage, Container>  = \.sharedContainer
    
    public let sharedContainer: Container
    
    public let privateContainer: SubContainer
    
    internal var hasActiveConnections: Bool
    
    public var ctx: ChannelHandlerContext
    
    
    
    public let ts: Date = Date()
    public let headers: MSG.Headers
    public let payload: Data
    public let sub: NatsSubscription?
    
    init(msg: MSG, container: Container, ctx: ChannelHandlerContext) {
        self.headers = msg.headers
        self.payload = msg.payload
        self.sub = msg.sub
        self.sharedContainer = container
        self.privateContainer = container.subContainer(on: container)
        hasActiveConnections = false
        self.ctx = ctx
    }
    
    
    public func databaseConnection<D>(to database: DatabaseIdentifier<D>?) -> Future<D.Connection> {
        guard let database = database else {
            let error = VaporError(
                identifier: "defaultDB",
                reason: "`Model.defaultDatabase` is required to use request as `DatabaseConnectable`.",
                suggestedFixes: [
                    "Ensure you are using the 'model' label when registering this model to your migration config (if it is a migration): migrations.add(model: ..., database: ...).",
                    "If the model you are using is not a migration, set the static `defaultDatabase` property manually in your app's configuration section.",
                    "Use `req.withPooledConnection(to: ...) { ... }` instead."
                ]
            )
            return eventLoop.newFailedFuture(error: error)
        }
        hasActiveConnections = true
        return privateContainer.requestCachedConnection(to: database, poolContainer: self)
    }
    
    
    
    public func decode<T: Codable>(type: T.Type) throws -> T{
        let decoder = JSONDecoder()
        return try decoder.decode(type, from: payload)
    }
    
    
    public var description: String {
        return """
        HEADERS:\n
        SUBJECT:    \(headers.subject)\n
        SID:        \(headers.sid)\n
        REPLY:      \(headers.reply ?? "")\n
        SIZE:       \(headers.size)\n
        PAYLOAD: \(String(bytes: payload, encoding: .utf8) ?? "PAYLOAD IS NOT UTF8")\n
        """
        
    }
    
    public var debugDescription: String {
        return description
    }
    
    
    
    deinit {
        debugPrint("DEINITIALIZING NATS MESSAGE")
        if hasActiveConnections {
            try! privateContainer.releaseCachedConnections()
        }
    }
    
    
    
}




enum NatsMessages {
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


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
    public let sub: NatsCallbacks?
    
    public struct Headers: Codable {
        public let subject: String
        public let sid: UUID
        public let reply: String?
        public let size: Int
    }
    
}

public protocol NatsResponder  {
    func publish(_ subject: String, payload: Data) throws -> EventLoopFuture<Void>
    func request(_ subject: String, payload: Data, timeout: Int, numberOfResponse: NatsRequest.NumberOfResponse) throws -> EventLoopFuture<NatsMessage>
    func unsubscribe()
}

public final class NatsMessage: ContainerAlias, DatabaseConnectable,  CustomStringConvertible, CustomDebugStringConvertible, NatsResponder, Logger  {
    public func unsubscribe() {
        
    }
    
    
    
    public func log(_ string: String, at level: LogLevel, file: String, function: String, line: UInt, column: UInt) {
        let text: String = "[ \(level) ] \(string)  --  (\(file):\(line))"
        debugPrint(text)
    }
    
    
    public func streamingSubscribe(_ subject: String, queueGroup: String = "", callback: @escaping ((_ T: NatsMessage) -> ())) throws -> EventLoopFuture<Void> {
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        return try handler.streamingSubscribe(subject, queueGroup: queueGroup, callback: callback)
    }
    @discardableResult
    public func subscribe(_ subject: String, queueGroup: String = "", callback: @escaping ((_ T: NatsMessage) -> ())) throws -> EventLoopFuture<Void>  {
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        return handler.subscribe(subject, queueGroup: queueGroup, callback: callback)
    }
    
    
    
    public func unsubscribe(_ subject: String, max: UInt32 = 0) throws -> EventLoopFuture<Void> {
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        return try handler.unsubscribe(subject, max: max)
    }
    
    /**
     * publish(subject: String) -> Void
     * publish to subject
     *
     */
    
    public func streamingPublish(_ subject: String, payload: Data) throws -> EventLoopFuture<Void> {
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        return try handler.streamingPub(subject, payload:payload)
    }
    
    public func publish(_ subject: String, payload: Data) throws -> EventLoopFuture<Void> {
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        return try handler.publish(subject, payload: payload)
    }
    
    /**
     * reply(subject: String, replyto: String, payload: String)  -> Void
     * reply to id in subject
     *
     */
    public func request(_ subject: String, payload: Data, timeout: Int, numberOfResponse: NatsRequest.NumberOfResponse) throws -> EventLoopFuture<NatsMessage> {
        guard let handler = ctx.handler as? NatsHandler else {
            let error = NatsGeneralError(identifier: "NATS Handler error", reason: "More likely incorrect thread")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        return try handler.request(subject, payload: payload, timeout: timeout, numberOfResponse: numberOfResponse)
    }
    
    
    
    public func steamingReply(payload: Data) throws -> EventLoopFuture<Void> {
        guard let streamingPayload = self.streamingPayload, !streamingPayload.reply.isEmpty else {
            let error = NatsGeneralError(identifier: "Not steaming message", reason: "This message is not streaming")
            throw error
        }
        
        guard !streamingPayload.reply.isEmpty else {
            let error = NatsGeneralError(identifier: "Replay string not found", reason: "This message does not have reply subject")
            throw error
        }
        
        return try self.publish(streamingPayload.reply, payload: payload)
    }
    
    public func reply(payload: Data) throws -> EventLoopFuture<Void> {
        guard let replyTopic = self.headers.reply else {
            let error = NatsGeneralError(identifier: "REPLY HEADERS DOES NOT EXISTS", reason: "THIS MESSAGE IS NOT A REQUEST, NO NEED TO RESPOND")
            throw error
        }
        return try self.publish(replyTopic, payload: payload)
    }
    
    public static var aliasedContainer: KeyPath<NatsMessage, Container>  = \.sharedContainer
    
    public let sharedContainer: Container
    
    public let privateContainer: SubContainer
    
    internal var hasActiveConnections: Bool
    
    public var ctx: ChannelHandlerContext
    
    
    
    public let ts: Date = Date()
    public let headers: MSG.Headers
    public let payload: Data
    public let streamingPayload: Pb_MsgProto?
    public let sub: NatsCallbacks?
    
    init(msg: MSG, container: Container, ctx: ChannelHandlerContext, streamingPayload: Pb_MsgProto? = nil) {
        self.headers = msg.headers
        self.payload = msg.payload
        self.streamingPayload = streamingPayload
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
        if hasActiveConnections {
            try! privateContainer.releaseCachedConnections()
        }
    }
    
    
    
}







public struct NatsError: Error {
    public let description: String?
}



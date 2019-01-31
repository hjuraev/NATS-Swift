//
//  StreamingHandler.swift
//  Nats
//
//  Created by Halimjon Juraev on 12/29/18.
//

import NIO
import SwiftProtobuf
import Vapor
import Bits

protocol NatsHandlerDelegate {
    func connected(ctx: ChannelHandlerContext)
    func added(ctx: ChannelHandlerContext)
    func open(ctx: ChannelHandlerContext)
    func streamingOpened(ctx: ChannelHandlerContext)
    func close(ctx: ChannelHandlerContext)
    func error(ctx: ChannelHandlerContext, error: Error)
}

public final class NatsHandler: ChannelInboundHandler {
    
    /// See `ChannelInboundHandler.InboundIn`
    public typealias InboundIn = NatsMessages
    
    /// See `ChannelInboundHandler.OutboundOut`
    public typealias OutboundOut = Data
    
    
    
    private var steamingConnectionRequest: Pb_ConnectRequest?
    private var steamingConnectionResponse: Pb_ConnectResponse?
    
    let config: NatsClientConfig
    
    public var ctx: ChannelHandlerContext?
    let container: Container
    
    var subscriptions: [UUID:NatsCallbacks] = [:]
    var servers: Server?
    
    let delegate: NatsRouter
    
    public init(container: Container, config: NatsClientConfig) throws {
        self.container = container
        self.delegate = try self.container.make(NatsRouter.self)
        self.config = config
    }
    
    public func handlerAdded(ctx: ChannelHandlerContext) {
        self.ctx = ctx
        
        
    }
    
    public func handlerRemoved(ctx: ChannelHandlerContext) {
        delegate.onClose(handler: self, ctx: ctx)
        
        switch config.disBehavior {
        case .doNotReconnect:
            break
        case .fatalCrash:
            fatalError("NATS DISCONNECTED -> CRASHING")
        case .reconnect:
            let bootstrap = ClientBootstrap(group: container.eventLoop)
                .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                .channelInitializer { channel in
                    return channel.pipeline.addHandlers([NatsEncoder(), NatsDecoder()], first: true)
            }
            if let server = config.natsServers.random{
                _ = bootstrap.connect(host: server.hostname, port: server.port).flatMap({ channel -> EventLoopFuture<Void> in
                    return channel.pipeline.add(handler: self)
                }).catch { (error) in
                    debugPrint(error)
                }
            }
        }
    }
    
    
    public func write(ctx:ChannelHandlerContext, data: Data) -> EventLoopFuture<Void>{
        return ctx.writeAndFlush(wrapOutboundOut(data))
    }
    
    /// See `ChannelInboundHandler.channelRead(ctx:data:)`
    public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
        let msg = unwrapInboundIn(data) as NatsMessages
        message(ctx: ctx, message: msg)
    }
    
    public func errorCaught(ctx: ChannelHandlerContext, error: Error) {
        delegate.onError(handler: self, ctx: ctx, error: error)
    }
    
    func error(ctx: ChannelHandlerContext, error: Error) {
        debugPrint("CHANNEL ERRROR: -> ", error)
    }
    
    /// See `ChannelInboundHandler.channelActive(ctx:)`
    public func channelActive(ctx: ChannelHandlerContext) {
        delegate.onOpen(handler: self, ctx: ctx)
        if config.streaming {
            var pb_request = Pb_ConnectRequest()
            pb_request.clientID = UUID().uuidString
            pb_request.heartbeatInbox = UUID().uuidString
            
            self.steamingConnectionRequest = pb_request
            let data = try! pb_request.serializedData()
            do {
                _ = try subscribe(pb_request.heartbeatInbox) { MSG in
                    _ = try? MSG.reply(payload: "".data(using: .utf8)!)
                }
                guard let clusterName = config.clusterName else {
                    debugPrint("Nats Cluster Name is not specified. Streaming connection can not be established")
                    return
                }
                _ = try request(clusterName, payload: data, timeout: 3, numberOfResponse: .single).map({ MSG -> Void in
                    self.steamingConnectionResponse = try Pb_ConnectResponse(serializedData: MSG.payload)
                    self.delegate.onStreamingOpen(handler: self, ctx: ctx)
                }).catch({ error in
                    debugPrint(error)
                })
                
            } catch {
                fatalError(error.localizedDescription)
            }
        }
    }
    
    public func unsubscribeStreamingRequest(sub: NatsCallbacks) throws -> EventLoopFuture<Void> {
        guard let connectRequest = self.steamingConnectionRequest, let connectResponse = self.steamingConnectionResponse else {
            let error = NatsGeneralError(identifier: "Connect Request or Connect Response is null", reason: "Please make sure you turned on streaming in Nats Config file")
            throw error
        }
        var unsub = Pb_UnsubscribeRequest()
        unsub.clientID = connectRequest.clientID
        unsub.subject = sub.subject
        unsub.inbox = sub.id.uuidString
        
        let payload = try unsub.serializedData()
        return try request(connectResponse.unsubRequests, payload: payload, timeout: 2, numberOfResponse: .single).map({ msg -> Void in
            return
        })
    }
    public func streamingCloseRequest() throws -> EventLoopFuture<Void> {
        guard let connectRequest = self.steamingConnectionRequest, let connectResponse = self.steamingConnectionResponse else {
            let error = NatsGeneralError(identifier: "Connect Request or Connect Response is null", reason: "Please make sure you turned on streaming in Nats Config file")
            throw error
        }
        
        var close = Pb_CloseRequest()
        close.clientID = connectRequest.clientID
        let payload = try close.serializedData()
        return try request(connectResponse.closeRequests, payload: payload, timeout: 20, numberOfResponse: .single).map({ msg -> Void in
            return
        })
        
    }
    public func streamingPub(_ subject: String, payload: Data) throws -> EventLoopFuture<Void> {
        guard let connectRequest = self.steamingConnectionRequest, let connectResponse = self.steamingConnectionResponse else {
            let error = NatsGeneralError(identifier: "Connect Request or Connect Response is null", reason: "Please make sure you turned on streaming in Nats Config file")
            throw error
        }
        
        let uuid = UUID()
        let inbox = UUID()
        var pub = Pb_PubMsg()
        pub.clientID = connectRequest.clientID
        pub.subject = subject
        pub.guid = inbox.uuidString
        pub.data = payload
        
        let payload = try pub.serializedData()
        guard let ctx = self.ctx else { throw NatsRequestError.coundNotFindChannelContextToUse}
        
        var finalData: Data = Data()
        let sub = "\(Proto.SUB.rawValue) \(inbox.uuidString) \(inbox.uuidString)\r\n\(Proto.UNSUB.rawValue) \(inbox.uuidString) \(2)\r\n".data(using: .utf8)!
        let publish = "\(Proto.PUB.rawValue) \(connectResponse.pubPrefix).\(subject) \(inbox.uuidString) \(payload.count)\r\n".data(using: .utf8)!
        
        finalData.append(sub)
        finalData.append(publish)
        finalData.append(payload)
        finalData.append(Byte.carriageReturn)
        finalData.append(Byte.newLine)
        
        let task = ctx.eventLoop.scheduleRepeatedTask(initialDelay: TimeAmount.seconds(0), delay: TimeAmount.seconds(2)) { (RepeatedTask) -> EventLoopFuture<Void> in
            return self.write(ctx: ctx, data: finalData)
        }
        
        let pubAck = PubMSG(pubMSG: pub, uuid: uuid, task: task)
        
        let callback = NatsCallbacks(id: inbox, subject: inbox.uuidString, queueGroup: "", callback: .pubAck(pubAck))
        subscriptions.updateValue(callback, forKey: inbox)
        
        return ctx.eventLoop.newSucceededFuture(result: Void())
    }
    
    public func streamingRequest(_ subject: String, payload: Data, timeout: Int, numberOfResponse: NatsRequest.NumberOfResponse) throws -> EventLoopFuture<NatsMessage> {
        guard let connectRequest = self.steamingConnectionRequest, let connectResponse = self.steamingConnectionResponse else {
            let error = NatsGeneralError(identifier: "Connect Request or Connect Response is null", reason: "Please make sure you turned on streaming in Nats Config file")
            throw error
        }
        
        let uuid = UUID()
        let inbox = UUID()
        let reply = UUID()
        var pub = Pb_PubMsg()
        pub.clientID = connectRequest.clientID
        pub.subject = subject
        pub.guid = inbox.uuidString
        pub.data = payload
        pub.reply = reply.uuidString
        
        let payload = try pub.serializedData()
        guard let ctx = self.ctx else { throw NatsRequestError.coundNotFindChannelContextToUse}
        
        var finalData: Data = Data()
        
        var numberOfResponses = ""
        switch numberOfResponse {
        case .multiple(let count):
            numberOfResponses = "\(Proto.UNSUB.rawValue) \(reply.uuidString) \(count.count)\r\n"
        case .single:
            numberOfResponses = "\(Proto.UNSUB.rawValue) \(reply.uuidString) \(1)\r\n"
        case .unlimited:
            numberOfResponses = "\(Proto.UNSUB.rawValue) \(reply.uuidString)\r\n"
        }
        
        let request = "\(Proto.SUB.rawValue) \(reply.uuidString) \(reply.uuidString)\r\n\(numberOfResponses)".data(using: .utf8)!
        let sub = "\(Proto.SUB.rawValue) \(inbox.uuidString) \(inbox.uuidString)\r\n\(Proto.UNSUB.rawValue) \(inbox.uuidString) \(1)\r\n".data(using: .utf8)!
        let publish = "\(Proto.PUB.rawValue) \(connectResponse.pubPrefix).\(subject) \(inbox.uuidString) \(payload.count)\r\n".data(using: .utf8)!
        
        finalData.append(request)
        finalData.append(sub)
        finalData.append(publish)
        finalData.append(payload)
        finalData.append(Byte.carriageReturn)
        finalData.append(Byte.newLine)
        
        let task = ctx.eventLoop.scheduleRepeatedTask(initialDelay: TimeAmount.seconds(0), delay: TimeAmount.seconds(2)) { (RepeatedTask) -> EventLoopFuture<Void> in
            return self.write(ctx: ctx, data: finalData)
        }
        
        let pubAck = PubMSG(pubMSG: pub, uuid: uuid, task: task)
        
        let callback = NatsCallbacks(id: inbox, subject: inbox.uuidString, queueGroup: "", callback: .pubAck(pubAck))
        
        let promise = ctx.eventLoop.newPromise(of: NatsMessage.self)
        
        let schedule = ctx.eventLoop.scheduleTask(in: .seconds(timeout), {
            self.subscriptions.removeValue(forKey: uuid)
            let error = NatsGeneralError(identifier: "NATS TIMEOUT", reason: "TIMEOUT SUBJECT: \(subject)")
            promise.fail(error: error)
        })
        
        let natsRequest = NatsRequest(promise: promise, scheduler: schedule, numberOfResponse: numberOfResponse)
        let responseCallback = NatsCallbacks(id: reply, subject: reply.uuidString, queueGroup: "", callback: .REQ(natsRequest))
        
        subscriptions.updateValue(responseCallback, forKey: reply)
        subscriptions.updateValue(callback, forKey: inbox)
        
        return promise.futureResult
    }
    
    public func streamingSubscribe(_ subject: String, queueGroup: String = "", callback: @escaping ((_ T: NatsMessage) -> ())) throws -> EventLoopFuture<Void> {
        guard let connectRequest = self.steamingConnectionRequest, let connectResponse = self.steamingConnectionResponse else {
            let error = NatsGeneralError(identifier: "Connect Request or Connect Response is null", reason: "Please make sure you turned on streaming in Nats Config file")
            throw error
        }
        let inbox = UUID()
        var subscribeRequest = Pb_SubscriptionRequest()
        subscribeRequest.clientID = connectRequest.clientID
        subscribeRequest.subject = subject
        subscribeRequest.qGroup = queueGroup
        subscribeRequest.inbox = inbox.uuidString
        subscribeRequest.maxInFlight = 1
        subscribeRequest.ackWaitInSecs = 2
        
        let data = try subscribeRequest.serializedData()
        
        let subscriptionString = "\(Proto.SUB.rawValue) \(inbox.uuidString) \(queueGroup)\(inbox.uuidString)\r\n".data(using: .utf8) ?? Data()
        
        guard let ctx = self.ctx else { throw NatsRequestError.coundNotFindChannelContextToUse}
        
        
        return self.write(ctx: ctx, data: subscriptionString).flatMap { Void -> EventLoopFuture<Void> in
            return try self.request(connectResponse.subRequests, payload: data, timeout: 4, numberOfResponse: .single).map { msg -> Void in
                let response = try Pb_SubscriptionResponse(serializedData: msg.payload)
                guard self.subscriptions.filter({ $0.value.subject == subject }).count == 0 else {
                    debugPrint("\(subject) -> ALREADY SUBSCRIBED")
                    return
                }
                let streamingSub = StreamingMSG(callback: callback, ackInbox: response.ackInbox)
                let sub = NatsCallbacks(id: inbox, subject: subject, queueGroup: queueGroup, callback: .StreamingMSG(streamingSub))
                self.subscriptions.updateValue(sub, forKey: inbox)
                
            }
        }
    }
    
    public func publish(_ subject: String, payload: Data) throws -> EventLoopFuture<Void> {
        guard let ctx = self.ctx else { throw NatsRequestError.coundNotFindChannelContextToUse}
        let pub = "\(Proto.PUB.rawValue) \(subject) \(payload.count)"
        guard var pubData = pub.data(using: .utf8) else {
            let error = NatsGeneralError(identifier: "Data parsing error", reason: "Could not convert String into Data, weird")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        pubData.append(Byte.carriageReturn)
        pubData.append(Byte.newLine)
        pubData.append(payload)
        pubData.append(Byte.carriageReturn)
        pubData.append(Byte.newLine)
        return write(ctx: ctx, data: pubData)
    }
    
    public func subscribe(_ subject: String, queueGroup: String = "", callback: @escaping ((_ T: NatsMessage) -> ())) throws -> EventLoopFuture<Void> {
        guard let ctx = self.ctx else { throw NatsRequestError.coundNotFindChannelContextToUse}
        guard subscriptions.filter({ $0.value.subject == subject }).count == 0 else {
            return ctx.eventLoop.newSucceededFuture(result: Void())
        }
        let uuid = UUID()
        let sub = NatsCallbacks(id: uuid, subject: subject, queueGroup: queueGroup, callback: .MSG(callback))
        subscriptions.updateValue(sub, forKey: uuid)
        return self.write(ctx: ctx, data: sub.sub())
    }
    
    
    public func unsubscribe(_ subject: String, max: UInt32 = 0) throws -> EventLoopFuture<Void>{
        guard let ctx = self.ctx else { throw NatsRequestError.coundNotFindChannelContextToUse}
        guard let sub = subscriptions.filter({ $0.value.subject == subject }).first else { return ctx.eventLoop.newSucceededFuture(result: Void()) }
        subscriptions.removeValue(forKey: sub.key)
        return write(ctx: ctx, data: sub.value.unsub(max))
    }
    
    public func request(_ subject: String, payload: Data, timeout: Int, numberOfResponse: NatsRequest.NumberOfResponse) throws -> EventLoopFuture<NatsMessage> {
        let uuid = UUID()
        
        guard let ctx = self.ctx else { throw NatsRequestError.coundNotFindChannelContextToUse}
        
        let promise = ctx.eventLoop.newPromise(of: NatsMessage.self)
        
        let schedule = ctx.eventLoop.scheduleTask(in: .seconds(timeout), {
            self.subscriptions.removeValue(forKey: uuid)
            let error = NatsGeneralError(identifier: "NATS TIMEOUT", reason: "TIMEOUT SUBJECT: \(subject)")
            promise.fail(error: error)
        })
        var finalData: Data = Data()
        let sub = "\(Proto.SUB.rawValue) \(uuid.uuidString) \(uuid.uuidString)\r\n\(Proto.UNSUB.rawValue) \(uuid.uuidString) \(1)\r\n".data(using: .utf8)!
        let request = NatsRequest(promise: promise, scheduler: schedule, numberOfResponse: numberOfResponse)
        let natsCallback = NatsCallbacks(id: uuid, subject: subject, queueGroup: "", callback: .REQ(request))
        subscriptions.updateValue(natsCallback, forKey: uuid)
        let pub = "\(Proto.PUB.rawValue) \(subject) \(uuid.uuidString) \(payload.count)\r\n"
        guard let pubData = pub.data(using: .utf8) else {
            let error = NatsGeneralError(identifier: "Data parsing error", reason: "Could not convert String into Data, weird")
            return ctx.eventLoop.newFailedFuture(error: error)
        }
        finalData.append(sub)
        finalData.append(pubData)
        finalData.append(payload)
        finalData.append(Byte.carriageReturn)
        finalData.append(Byte.newLine)
        
        return self.write(ctx: ctx, data: finalData).flatMap({ Void -> EventLoopFuture<NatsMessage>  in
            return promise.futureResult
        })
    }
    
    
    
    
    
    
    
    fileprivate func processPing(ctx:ChannelHandlerContext) {
        _ = write(ctx: ctx, data: "\(Proto.PONG.rawValue)\r\n".data(using: .utf8) ?? Data())
    }
    
    func message(ctx: ChannelHandlerContext, message: NatsMessages) {
        switch message {
        case .OK:
            break
        case .PING:
            self.processPing(ctx: ctx)
            break
        case .PONG:
            break
        case .ERR(let error):
            delegate.onError(handler: self, ctx: ctx, error: error)
            break
        case .INFO(let server):
            self.servers = server
            break
        case .MSG(let message):
            do {
                guard let subscription = subscriptions[message.headers.sid] else {
                    debugPrint("Received message without subscription")
                    return
                }
                
                switch subscription.callback {
                case .MSG(let callback):
                    
                    let newMessage = NatsMessage(msg: message, container: container, ctx: ctx)
                    callback(newMessage)
                case .REQ(let promise):
                    let newMessage = NatsMessage(msg: message, container: container, ctx: ctx)
                    promise.scheduler?.cancel()
                    
                    switch promise.numberOfResponse {
                    case .single:
                        subscriptions.removeValue(forKey: message.headers.sid)
                    case .multiple(let count):
                        if !count.counter() {
                            subscriptions.removeValue(forKey: message.headers.sid)
                        }
                    case .unlimited:
                        break
                    }
                    promise.promise.succeed(result: newMessage)
                case .pubAck(let proto):
                    let pb_ack = try Pb_PubAck(serializedData: message.payload)
                    if proto.pubMSG.guid == pb_ack.guid {
                        proto.task.cancel()
                        subscriptions.removeValue(forKey: message.headers.sid)
                    }
                case .StreamingMSG(let streamMSG):
                    let msgProto = try Pb_MsgProto(serializedData: message.payload)
                    var ack = Pb_Ack()
                    ack.sequence = msgProto.sequence
                    ack.subject = msgProto.subject
                    
                    let data = try ack.serializedData()
                    
                    try publish(streamMSG.ackInbox, payload: data).map({ Void -> Void in
                        
                        let newMessage = NatsMessage(msg: message, container: self.container, ctx: ctx, streamingPayload: msgProto)
                        streamMSG.callback(newMessage)
                    }).catch({ error in
                        debugPrint(error)
                    })
                    break
                }
            } catch {
                debugPrint(error)
            }
            break
        }
    }
    
    
    
    
}


public protocol NatsRouter {
    func onOpen(handler: NatsHandler, ctx: ChannelHandlerContext)
    func onStreamingOpen(handler: NatsHandler,  ctx: ChannelHandlerContext)
    func onClose(handler: NatsHandler, ctx: ChannelHandlerContext)
    func onError(handler: NatsHandler, ctx: ChannelHandlerContext, error: Error)
}

enum NatsRequestError: Error {
    case TIMEOUT
    case couldNotFindMainContainer
    case coundNotFindContainerToUse
    case couldNotFetchRequestStorage
    case coundNotFindChannelContextToUse
    case NatsClientNotAttachedToThisRequest
}


enum StreamingProtocol {
    case ConnectRequest(ConnectRequest)
    case ConnectResponse
    case SubscriptionRequest
    case SubscriptionResponse
    case UnsubscribeRequest
    case PubMsg
    case PubAck
    case MsgProto
    case Ack
    case CloseRequest
    case CloseResp
}

public enum NatsMessages {
    case MSG(MSG)
    case INFO(Server)
    case OK
    case ERR(NatsError)
    case PONG
    case PING
}

private final class ThreadContainer {
    var container: Container
    init(container: Container) {
        self.container = container
    }
}

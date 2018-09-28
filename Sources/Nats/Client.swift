//
//  Client.swift
//  Run
//
//  Created by Halimjon Juraev on 2/2/18.
//

import Foundation
import NIO
import Logging
import Vapor
import Bits


public struct natsServer: Codable {
    let hostname: String
    let port: Int
    public init(hostname: String, port: Int = 4222) {
        self.hostname = hostname
        self.port = port
    }
}

public enum natsConnectionType: String,  Codable {
    case sequential = "sequential"
    case random = "random"
}

public struct NatsClientConfig {
    public let natsServers: [natsServer]
    public let connectionType: natsConnectionType
    public let threadNum: Int
    
    /// Create a new `NatsClientConfig`
    /// NOTE: For each thread it creates separate socket connection to NAT server
    public init(servers: [natsServer], connectionType:natsConnectionType, threadNum: Int) {
        self.natsServers = servers
        self.connectionType = connectionType
        self.threadNum = threadNum
        
    }
}

public protocol NatsClientDelegate {
    func open(client: NatsClient, ctx: ChannelHandlerContext)
    func close(client: NatsClient, ctx: ChannelHandlerContext)
    func error(client: NatsClient, ctx: ChannelHandlerContext, error: NatsError)
}


public final class NatsClient:NatsHandlerDelegate, Service {
    
    public var delegate: NatsClientDelegate?
    
    private let handler: NatsHandler
    
    fileprivate let containerCache = ThreadSpecificVariable<ThreadContainer>()
    fileprivate let channelContextCache = ThreadSpecificVariable<ChannelHandlerContext>()
    
    
    fileprivate let threadGroup: MultiThreadedEventLoopGroup
    
    fileprivate let bootstrap: ClientBootstrap
    public let container: Container
    
    public init(natsConfig: NatsClientConfig, delegate: NatsClientDelegate, container: Container) {
        
        self.delegate = delegate
        threadGroup = MultiThreadedEventLoopGroup(numberOfThreads: natsConfig.threadNum)
        let handler = NatsHandler()
        self.handler = handler
        self.container = container
        
        bootstrap = ClientBootstrap(group: threadGroup)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .channelInitializer { channel in
                return channel.pipeline.addHandlers([NatsEncoder(), NatsDecoder()], first: true).then({
                    channel.pipeline.add(handler: handler)
                })
        }
        self.handler.delegate = self
        for _ in 0..<natsConfig.threadNum {
            // initialize each event loop
            if let server = natsConfig.natsServers.random{
                _ =  self.bootstrap.connect(host: server.hostname, port: server.port)
            }
        }
        
        
        
        
    }
    
    
    
    
    
    func message(ctx: ChannelHandlerContext, message: NatsMessages) {
        
        switch message {
        case .OK:
//            debugPrint("OK")
            break
        case .PING:
            self.processPing(ctx: ctx)
            break
        case .PONG:
            break
        case .ERR(let error):
            delegate?.error(client: self, ctx: ctx, error: error)
            break
        case .INFO(let server):
            do {
                
                guard let container = containerCache.currentValue?.container else { throw NatsRequestError.coundNotFindContainerToUse}
                let storage = try container.make(NatsResponseStorage.self)
                storage.servers = [server]
            } catch {
                debugPrint(error)
            }
            break
        case .MSG(let message):
            
            
            do {
                guard let container = containerCache.currentValue?.container else { throw NatsRequestError.coundNotFindContainerToUse}
                
                let storage = try container.make(NatsResponseStorage.self)
                if !storage.requestsStorage.isEmpty, let request = storage.requestsStorage[message.headers.sid] {
                    let newMessage = NatsMessage(msg: message, container: container, ctx: ctx)
                    request.promise.succeed(result: newMessage)
                    storage.requestsStorage.removeValue(forKey: message.headers.sid)
                } else if let sub = storage.subscriptions[message.headers.sid] {
                    let newMessage = NatsMessage(msg: message, container: container, ctx: ctx)
                    sub.callback?(newMessage)
                } else {
                    debugPrint("Received message without subscription")
                }
            } catch {
                debugPrint(error)
            }
            break
        }
    }
    
    func added(ctx: ChannelHandlerContext) {
        
    }
    
    func open(ctx: ChannelHandlerContext) {
        let subContainer = container.subContainer(on: ctx.eventLoop)
        self.containerCache.currentValue = ThreadContainer(container: subContainer)
        channelContextCache.currentValue = ctx
        delegate?.open(client: self, ctx: ctx)
        
    }
    
    func close(ctx: ChannelHandlerContext) {
        delegate?.close(client: self, ctx: ctx)
    }
    
    func error(ctx: ChannelHandlerContext, error: Error) {
        debugPrint("CHANNEL ERRROR: -> ", error)
        //        delegate.error(ctx: ctx, error: NatsError)
    }
    
    
    
    fileprivate func processPing(ctx:ChannelHandlerContext) {
        _ = self.handler.write(ctx: ctx, data: "\(Proto.PONG.rawValue)\r\n".data(using: .utf8) ?? Data())
    }
    
    
    public func subscribe(_ subject: String, queueGroup: String = "", callback: ((_ T: NatsMessage) -> ())?) throws -> Void {
        guard let ctx = channelContextCache.currentValue else { throw NatsRequestError.coundNotFindChannelContextToUse}
        
        
        guard let container = containerCache.currentValue?.container else { throw NatsRequestError.coundNotFindContainerToUse}
        
        
        let storage = try container.make(NatsResponseStorage.self)
        
        guard storage.subscriptions.filter({ $0.value.subject == subject }).count == 0 else { return }
        let uuid = UUID()
        let sub = NatsSubscription(id: uuid, subject: subject, queueGroup: queueGroup, count: 0, callback: callback)
        
        storage.subscriptions.updateValue(sub, forKey: uuid)
        _ = self.handler.write(ctx: ctx, data: sub.sub())
    }
    
    
    
    public func unsubscribe(_ subject: String, max: UInt32 = 0) throws {
        guard let ctx = channelContextCache.currentValue else { throw NatsRequestError.coundNotFindChannelContextToUse}
        guard let container = containerCache.currentValue?.container else { throw NatsRequestError.coundNotFindContainerToUse}
        
        
        let storage = try container.make(NatsResponseStorage.self)
        
        guard let sub = storage.subscriptions.filter({ $0.value.subject == subject }).first else { return }
        storage.subscriptions.removeValue(forKey: sub.key)
        
        _ = self.handler.write(ctx: ctx, data: sub.value.unsub(max))
    }
    
    //    /**
    //     * publish(subject: String) -> Void
    //     * publish to subject
    //     *
    //     */
    //    public func publish(_ subject: String, payload: String) throws -> EventLoopFuture<Void> {
    //        let pub: () -> Data = {
    //            if let data = payload.data(using: String.Encoding.utf8) {
    //                return "\(Proto.PUB.rawValue) \(subject) \(data.count)\r\n\(payload)\r\n".data(using: .utf8) ?? Data()
    //            }
    //            return Data()
    //        }
    //        guard let ctx = channelContextCache.currentValue else { throw NatsRequestError.coundNotFindChannelContextToUse}
    //
    //        return self.handler.write(ctx: ctx, data: pub())
    //    }
    //
    //    /**
    //     * reply(subject: String, replyto: String, payload: String)  -> Void
    //     * reply to id in subject
    //     *
    //     */
    //    public func request(_ subject: String, payload: String, timeout: Int) throws -> EventLoopFuture<NatsMessage> {
    //        let uuid = UUID()
    //
    //        guard let ctx = channelContextCache.currentValue else { throw NatsRequestError.coundNotFindChannelContextToUse}
    //
    //        let promise = ctx.eventLoop.newPromise(NatsMessage.self)
    //
    //        guard let container = containerCache.currentValue?.container else { throw NatsRequestError.coundNotFindContainerToUse}
    //
    //        let storage = try container.make(NatsResponseStorage.self)
    //
    //        let schedule = ctx.eventLoop.scheduleTask(in: .seconds(timeout), {
    //            promise.fail(error: NatsRequestError.TIMEOUT)
    //
    //            storage.requestsStorage.removeValue(forKey: uuid)
    //        })
    //
    //        let sub = "\(Proto.SUB.rawValue) \(uuid.uuidString) \(uuid.uuidString)\r\n\(Proto.UNSUB.rawValue) \(uuid.uuidString) \(1)\r\n".data(using: .utf8)
    //        self.handler.write(ctx: ctx, data: sub ?? Data())
    //
    //        let request = NatsRequest(id: uuid, subject: subject, promise: promise, scheduler: schedule)
    //
    //        storage.requestsStorage.updateValue(request, forKey: uuid)
    //        let requestSocket: () -> Data = {
    //            if let data = payload.data(using: String.Encoding.utf8) {
    //                return "\(Proto.PUB.rawValue) \(subject) \(uuid.uuidString) \(data.count)\r\n\(payload)\r\n".data(using: .utf8) ?? Data()
    //            }
    //            return Data()
    //        }
    //        self.handler.write(ctx: ctx, data: requestSocket()).catch { error in
    //            debugPrint(error)
    //        }
    //        return promise.futureResult
    //    }
    
    enum NatsRequestError: Error {
        case TIMEOUT
        case couldNotFindMainContainer
        case coundNotFindContainerToUse
        case couldNotFetchRequestStorage
        case coundNotFindChannelContextToUse
        case NatsClientNotAttachedToThisRequest
    }
    
}


private final class ThreadContainer {
    var container: SubContainer
    init(container: SubContainer) {
        self.container = container
    }
}

//extension Thread {
//    func cachedChannelHandler() throws -> ChannelHandlerContext {
//
//        enum ChannelContextError: Error {
//            case CouldNotFindChannelContextInThisThread
//        }
//
//        if let ctx = threadDictionary["ctx"] as? ChannelHandlerContext {
//            return ctx
//        } else {
//            throw ChannelContextError.CouldNotFindChannelContextInThisThread
//        }
//
//
//    }
//
//
//    func cachedSubContainer(for container: Container, on worker: Worker) -> SubContainer {
//        let subContainer: SubContainer
//        if let existing = threadDictionary["subcontainer"] as? SubContainer {
//            subContainer = existing
//        } else {
//            let new = container.subContainer(on: worker)
//            subContainer = new
//            threadDictionary["subcontainer"] = new
//        }
//        return subContainer
//    }
//
//    public var subContainer: SubContainer? {
//        get {
//            return threadDictionary["subcontainer"] as? SubContainer
//        }
//    }
//}


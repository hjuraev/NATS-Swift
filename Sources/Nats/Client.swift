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




public struct NatsClientConfig: Codable {
    public static func makeService(for worker: Container) throws -> NatsClientConfig {
        return .default()
    }
    

    
    /// Default `RedisClientConfig`
    public static func `default`() -> NatsClientConfig {
        return .init(hostname: "localhost", port: 4222, threadNum: 4)
    }
    
    /// The Redis server's hostname.
    public var hostname: String
    
    /// The Redis server's port.
    public var port: Int
    public var threadNum: Int
    
    /// Create a new `RedisClientConfig`
    /// NOTE: For each thread it creates separate socket connection to NAT server
    public init(hostname: String, port: Int, threadNum: Int) {
        self.hostname = hostname
        self.port = port
        self.threadNum = threadNum
    }
}

public protocol NatsClientDelegate {
    func open(ctx: ChannelHandlerContext)
    func close(ctx: ChannelHandlerContext)
    func error(ctx: ChannelHandlerContext, error: NatsError)
}


public final class NatsClient:NatsHandlerDelegate, Container {
    
    public var config: Config
    
    public var environment: Environment
    
    public var services: Services
    
    public var serviceCache: ServiceCache
    
    public var delegate: NatsClientDelegate?

    private let handler: NatsHandler
    
    public var server: [Server] = []
    
    fileprivate let threadGroup: MultiThreadedEventLoopGroup
    
    public init(natsConfig: NatsClientConfig, _ config: inout Config, _ env: inout Environment, _ services: inout Services) {
        threadGroup = MultiThreadedEventLoopGroup(numThreads: natsConfig.threadNum)
        let handler = NatsHandler()
        self.handler = handler

        let bootstrap = ClientBootstrap(group: threadGroup)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .channelInitializer { channel in
                return channel.pipeline.addHandlers([NatsEncoder(), NatsDecoder()], first: true).then({
                    channel.pipeline.add(handler: handler)
                })
        }
        for _ in 0..<natsConfig.threadNum {
            _ = bootstrap.connect(host: natsConfig.hostname, port: natsConfig.port)
        }
        self.config = config
        self.environment = env
        self.services = services
        self.serviceCache = .init()
        self.handler.delegate = self
    }



    
    
    func message(ctx: ChannelHandlerContext, message: NatsMessage) {
        switch message {
        case .OK:
            debugPrint("OK")
            break
        case .PING:
            self.processPing(ctx: ctx)
            break
        case .PONG:
            break
        case .ERR(let error):
            delegate?.error(ctx: ctx, error: error)
            break
        case .INFO(let server):
//            self.server.append(server)
            break
        case .MSG(let message):
            let subContainer = Thread.current.cachedSubContainer(for: self, on: ctx.eventLoop)
            
            if let storage = try? subContainer.make(NatsResponseStorage.self) {
                if !storage.requestsStorage.isEmpty, let request = storage.requestsStorage[message.headers.sid] {
                    request.promise.succeed(result: message)
                    storage.requestsStorage.removeValue(forKey: message.headers.sid)
                } else if let sub = storage.subscriptions[message.headers.sid] {
                    sub.callback?(message)
                } else {
                    debugPrint("Received message without subscription")
                }
            }
            break
        }
    }
    
    func added(ctx: ChannelHandlerContext) {
    }
    
    func open(ctx: ChannelHandlerContext) {
        Thread.current.threadDictionary["ctx"] = ctx

        debugPrint("NATS open on \(ctx.channel.localAddress?.description ?? "")")
        delegate?.open(ctx: ctx)
    }
    
    func close(ctx: ChannelHandlerContext) {
        delegate?.close(ctx: ctx)

    }
    
    func error(ctx: ChannelHandlerContext, error: Error) {
//        delegate.error(ctx: ctx, error: NatsError)
    }
    
    
    
    fileprivate func processPing(ctx:ChannelHandlerContext) {
        self.handler.write(ctx: ctx, data: "\(Proto.PONG.rawValue)\r\n".data(using: .utf8) ?? Data())
    }


    public func subscribe(_ subject: String, queueGroup: String = "", callback: ((MSG) -> ())?) throws -> Void {
        let ctx = try Thread.current.cachedChannelHandler()
        let subContainer = Thread.current.cachedSubContainer(for: self, on: ctx.eventLoop)

        guard let storage = try? subContainer.make(NatsResponseStorage.self) else {return}

        guard storage.subscriptions.filter({ $0.value.subject == subject }).count == 0 else { return }
        let uuid = UUID()
        let sub = NatsSubscription(id: uuid, subject: subject, queueGroup: queueGroup, count: 0, callback: callback)
        
        storage.subscriptions.updateValue(sub, forKey: uuid)
        

        self.handler.write(ctx: ctx, data: sub.sub())
    }


    
    public func unsubscribe(_ subject: String, max: UInt32 = 0) throws {
        let ctx = try Thread.current.cachedChannelHandler()
        let subContainer = Thread.current.cachedSubContainer(for: self, on: ctx.eventLoop)
        guard let storage = try? subContainer.make(NatsResponseStorage.self) else {return}
        
        guard let sub = storage.subscriptions.filter({ $0.value.subject == subject }).first else { return }
        storage.subscriptions.removeValue(forKey: sub.key)

        self.handler.write(ctx: ctx, data: sub.value.unsub(max))
    }
    
    /**
     * publish(subject: String) -> Void
     * publish to subject
     *
     */
    public func publish(_ subject: String, payload: String) throws {
        let pub: () -> Data = {
            if let data = payload.data(using: String.Encoding.utf8) {
                return "\(Proto.PUB.rawValue) \(subject) \(data.count)\r\n\(payload)\r\n".data(using: .utf8) ?? Data()
            }
            return Data()
        }
        let ctx = try Thread.current.cachedChannelHandler()
        self.handler.write(ctx: ctx, data: pub())
    }
    
    /**
     * reply(subject: String, replyto: String, payload: String)  -> Void
     * reply to id in subject
     *
     */
    public func request(_ subject: String, payload: String, timeout: Int) throws -> EventLoopFuture<MSG> {
        let uuid = UUID()
        
        let ctx = try Thread.current.cachedChannelHandler()

        let promise = ctx.eventLoop.newPromise(MSG.self)

        let subContainer = Thread.current.cachedSubContainer(for: self, on: ctx.eventLoop)
        guard let storage = try? subContainer.make(NatsResponseStorage.self) else { throw NatsRequestError.couldNotFetchRequestStorage}
        
        let schedule = ctx.eventLoop.scheduleTask(in: .seconds(timeout), {
            promise.fail(error: NatsRequestError.TIMEOUT)
            storage.requestsStorage.removeValue(forKey: uuid)
        })
        let sub = "\(Proto.SUB.rawValue) \(uuid.uuidString) \(uuid.uuidString)\r\n\(Proto.UNSUB.rawValue) \(uuid.uuidString) \(1)\r\n".data(using: .utf8)
        self.handler.write(ctx: ctx, data: sub ?? Data())
        
        let request = NatsRequest(id: uuid, subject: subject, promise: promise, scheduler: schedule)
        
        storage.requestsStorage.updateValue(request, forKey: uuid)
        let requestSocket: () -> Data = {
            if let data = payload.data(using: String.Encoding.utf8) {
                return "\(Proto.PUB.rawValue) \(subject) \(uuid.uuidString) \(data.count)\r\n\(payload)\r\n".data(using: .utf8) ?? Data()
            }
            return Data()
        }
        self.handler.write(ctx: ctx, data: requestSocket())
        return promise.futureResult
    }
    
    enum NatsRequestError: Error {
        case TIMEOUT
        case couldNotFindMainContainer
        case couldNotFetchRequestStorage
    }
    
}

extension Thread {
    func cachedChannelHandler() throws -> ChannelHandlerContext {
        
        enum ChannelContextError: Error {
            case CouldNotFindChannelContextInThisThread
        }
        
        if let ctx = threadDictionary["ctx"] as? ChannelHandlerContext {
            return ctx
        } else {
            throw ChannelContextError.CouldNotFindChannelContextInThisThread
        }
        

    }
    
    
    func cachedSubContainer(for container: Container, on worker: Worker) -> SubContainer {
        let subContainer: SubContainer
        if let existing = threadDictionary["subcontainer"] as? SubContainer {
            subContainer = existing
        } else {
            let new = container.subContainer(on: worker)
            subContainer = new
            threadDictionary["subcontainer"] = new
        }
        return subContainer
    }
    
    public var subContainer: SubContainer? {
        get {
            return threadDictionary["subcontainer"] as? SubContainer
        }
    }
}


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




public struct NatsClientConfig: Codable, ServiceType {
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
    public init(hostname: String, port: Int, threadNum: Int) {
        self.hostname = hostname
        self.port = port
        self.threadNum = threadNum
    }
}



public final class NatsClient: Service {

    private let handler: NatsHandler
    
//    let parser: TranslatingStreamWrapper<NatsParser>
    fileprivate var subscriptions = [UUID:NatsSubscription]()
    fileprivate var server: Server?
    fileprivate var requestsStorage: [UUID:NatsRequest] = [:]
    fileprivate let threadGroup: MultiThreadedEventLoopGroup
    let mainContainer: Container

    public static func connect(config: NatsClientConfig, on worker: Container, onError: @escaping (Error) -> Void, onOpen: @escaping () -> Void) throws -> Future<NatsClient> {
        let handler = NatsHandler(on: worker, onError: onError, onOpen: onOpen)
        let bootstrap = ClientBootstrap(group: worker.eventLoop)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .channelInitializer { channel in
                return channel.pipeline.addHandlers([NatsEncoder(), NatsDecoder()], first: true).then({
                    channel.pipeline.add(handler: handler)
                })
        }
        return bootstrap.connect(host: config.hostname, port: config.port).map(to: NatsClient.self, { _ in
            return .init(queue: handler, container: worker, threadNum: config.threadNum)
        })
    }
    
    
    init(queue: NatsHandler, container: Container, threadNum: Int) {
        self.handler = queue
        self.mainContainer = container
        threadGroup = MultiThreadedEventLoopGroup(numThreads: threadNum)
        listenSocket()
    }

    public var onError: ((NatsError) -> ())?
    public var onInfo: ((Server) -> ())?
    public var onClose: (() -> ())?
    

    
    public func listenSocket() {
        self.handler.onClose = {
            self.onClose?()
        }
        
        self.handler.onNatsMessage = { msg in
            let container = Thread.current.cachedSubContainer(for: self.mainContainer, on: self.threadGroup.next())
            container.eventLoop.execute {
                switch msg {
                case .OK:
                    break
                case .PING:
                    self.processPing()
                    break
                case .PONG:
                    
                    break
                case .ERR(let error):
                    self.onError?(error)
                    break
                case .INFO(let server):
                    self.onInfo?(server)
                    self.server = server
                    break
                case .MSG(let message):
                    var message = message
                    message.container = container
                    if !self.requestsStorage.isEmpty, let request = self.requestsStorage[message.headers.sid] {
                        request.promise.succeed(result: message)
                        self.requestsStorage.removeValue(forKey: message.headers.sid)
                        return
                    }else if let sub = self.subscriptions[message.headers.sid] {
                        sub.callback?(message)
                    } else {
                        debugPrint("Received message without subscription")
                    }
                    break
                }
            }
        }
    }
    
    fileprivate func processPing() {
        print("Sending Pong")
        _ = self.handler.enqueue("\(Proto.PONG.rawValue)\r\n")
    }


    open func subscribe(_ subject: String, queueGroup: String = "", callback: ((MSG) -> ())?) -> Void {
        guard subscriptions.filter({ $0.value.subject == subject }).count == 0 else { return }
        let uuid = UUID()
        let sub = NatsSubscription(id: uuid, subject: subject, queueGroup: queueGroup, count: 0, callback: callback)
        subscriptions.updateValue(sub, forKey: uuid)
        _ = self.handler.enqueue(sub.sub())
    }


    
    open func unsubscribe(_ subject: String, max: UInt32 = 0) {
        guard let sub = subscriptions.filter({ $0.value.subject == subject }).first else { return }
        subscriptions.removeValue(forKey: sub.key)
        _ = self.handler.enqueue(sub.value.unsub(max))
    }
    
    /**
     * publish(subject: String) -> Void
     * publish to subject
     *
     */
    open func publish(_ subject: String, payload: String) {
        let pub: () -> String = {
            if let data = payload.data(using: String.Encoding.utf8) {
                return "\(Proto.PUB.rawValue) \(subject) \(data.count)\r\n\(payload)\r\n"
            }
            return ""
        }
        _ = self.handler.enqueue(pub())
    }
    
    /**
     * reply(subject: String, replyto: String, payload: String)  -> Void
     * reply to id in subject
     *
     */
    open func request(_ subject: String, replyto: String, payload: String) -> EventLoopFuture<MSG>? {
        let uuid = UUID()

        guard let context = handler.currentCtx else {return nil}
        let promise = context.eventLoop.newPromise(MSG.self)

        
        let schedule = context.eventLoop.scheduleTask(in: .seconds(2), {
            promise.fail(error: NatsRequestError.TIMEOUT)
            _ = self.requestsStorage.removeValue(forKey: uuid)
        })
  
        let request = NatsRequest(id: uuid, subject: subject, promise: promise, scheduler: schedule)
        
        requestsStorage.updateValue(request, forKey: uuid)

        let requestSocket: () -> String = {
            if let data = payload.data(using: String.Encoding.utf8) {
                return "\(Proto.PUB.rawValue) \(subject) \(replyto) \(data.count)\r\n\(payload)\r\n"
            }
            return ""
        }
        _ = self.handler.enqueue(requestSocket())
        return promise.futureResult
    }
    
    enum NatsRequestError: Error {
        case TIMEOUT
    }
    
}

extension Thread {
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
}


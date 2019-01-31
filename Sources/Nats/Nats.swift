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
import Dispatch


public struct natsServer: Codable {
    let hostname: String
    let port: Int
    public init(hostname: String, port: Int = 4222) {
        self.hostname = hostname
        self.port = port
    }
}

public enum natsConnectionType {
    case server(numberOfThreads)
    case client
}

public enum disconnectBehavior {
    case reconnect
    case doNotReconnect
    case fatalCrash
}

public enum numberOfThreads {
    case single
    case multiple(Int)
    case activeProcessorCount
}

public struct NatsClientConfig: Service {
    public let natsServers: [natsServer]
    public let natsType: natsConnectionType
    public let disBehavior: disconnectBehavior
    public let streaming: Bool
    public var clusterName: String? = nil
    
    /// Create a new `NatsClientConfig`
    /// NOTE: For each thread it creates separate socket connection to NAT server
    public init(servers: [natsServer], connectionType:natsConnectionType, disBehavior: disconnectBehavior, clusterName: String?, streaming: Bool = false) {
        self.clusterName = clusterName
        self.natsServers = servers
        self.natsType = connectionType
        self.disBehavior = disBehavior
        self.streaming = streaming
    }
}



public final class NATS: Service {
    
    fileprivate let handlerCacher = ThreadSpecificVariable<NatsHandler>()
    fileprivate let config: NatsClientConfig
    fileprivate let container: Container
    
    var isActive: Bool {
        return handlerCacher.currentValue?.ctx?.channel.isWritable ?? false
    }
    
    public func getClient() -> EventLoopFuture<NATS> {
        if isActive {
            return container.eventLoop.future(self)
        }
        switch config.natsType {
        case .client:
            let bootstrap = ClientBootstrap(group: container.eventLoop)
                .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                .channelInitializer { channel in
                    return channel.pipeline.addHandlers([NatsEncoder(), NatsDecoder()], first: true)
            }
            guard let server = config.natsServers.random else {
                let error = NatsGeneralError(identifier: "Can not find server in config", reason: "You didnt specify Nats servers in config instance")
                return container.eventLoop.newFailedFuture(error: error)
            }
            let completion = bootstrap.connect(host: server.hostname, port: server.port).flatMap({ channel -> EventLoopFuture<Void> in
                print("GOT CHANNEL")
                let handler = try NatsHandler(container: self.container, config: self.config)
                self.handlerCacher.currentValue = handler
                
                return channel.pipeline.add(handler: handler)
            }).catch({ error in
                debugPrint(error)
            })
            return completion.map { Void -> NATS in
                return self
            }
        case .server(_):
            let error = NatsGeneralError(identifier: "Incorrectly configured", reason: "If you want to start it as client, please specify it in config instance")
            return container.eventLoop.newFailedFuture(error: error)
        }
    }
    
    public func startServer() -> EventLoopFuture<Void> {
        let threadGroup: MultiThreadedEventLoopGroup
        let threadCount: Int
        switch config.natsType {
        case .client:
            let error = NatsGeneralError(identifier: "Incorrectly configured", reason: "If you want to start it as server, please specify it in config instance")
            return container.eventLoop.newFailedFuture(error: error)
        case .server(let threads):
            switch threads {
            case .multiple(let count):
                threadCount = count
                threadGroup = MultiThreadedEventLoopGroup(numberOfThreads: count)
            case .single:
                threadCount = 1
                threadGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            case .activeProcessorCount:
                threadCount = ProcessInfo.processInfo.activeProcessorCount
                threadGroup = MultiThreadedEventLoopGroup(numberOfThreads: ProcessInfo.processInfo.activeProcessorCount)
            }
            
            let bootstrap = ClientBootstrap(group: threadGroup)
                .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                .channelInitializer { channel in
                    return channel.pipeline.addHandlers([NatsEncoder(), NatsDecoder()], first: true)
            }
            
            
            
            let signalQueue = DispatchQueue(label: "vapor.jobs.command.SignalHandlingQueue")
            
            //SIGTERM
            let termSignalSource = DispatchSource.makeSignalSource(signal: SIGTERM, queue: signalQueue)
            termSignalSource.setEventHandler {
                print("Shutting down remaining jobs.")
                termSignalSource.cancel()
                for _ in 0..<threadCount {
                    let eventLoop = threadGroup.next()
                    eventLoop.execute({
                        do {
                            if let handler = self.handlerCacher.currentValue {
                                for (x, value) in handler.subscriptions {
                                    switch value.callback {
                                    case .REQ(_):
                                        break
                                    case .StreamingMSG(_):
                                        _ = try handler.streamingCloseRequest()
                                        break
                                    default:
                                        _ = try self.publishRaw(payload: value.unsub(0))
                                        break
                                    }
                                    handler.subscriptions.removeValue(forKey: x)
                                }
                            }
                            
                        }catch {
                            debugPrint(error)
                        }
                    })
                }
                try! threadGroup.syncShutdownGracefully()
                fatalError("Closed down everything")
            }
            signal(SIGTERM, SIG_IGN)
            termSignalSource.resume()
            
            
            var completions: [EventLoopFuture<Void>] = []
            for _ in 0..<threadCount {
                // initialize each event loop
                guard let server = config.natsServers.random else {
                    let error = NatsGeneralError(identifier: "Can not find server in config", reason: "You didnt specify Nats servers in config instance")
                    return container.eventLoop.newFailedFuture(error: error)
                }
                let completion = bootstrap.connect(host: server.hostname, port: server.port).flatMap({ channel -> EventLoopFuture<Void> in
                    let subContainer = self.container.subContainer(on: channel.eventLoop)
                    let handler = try NatsHandler(container: subContainer, config: self.config)
                    self.handlerCacher.currentValue = handler
                    return channel.pipeline.add(handler: handler)
                }).catch { (error) in
                    debugPrint(error)
                }
                completions.append(completion)
            }
            
            return completions.flatten(on: container)
        }
        
        
        
    }
    
    public init(container: Container, config: NatsClientConfig) throws {
        self.container = container
        self.config = config
    }
    
    public func streamingPublish(_ subject: String, payload: Data) throws -> EventLoopFuture<Void> {
        guard let handler = handlerCacher.currentValue else {throw handlerError()}
        return try handler.streamingPub(subject,payload:payload)
    }
    
    public func streamingRequest(_ subject: String, payload: Data, timeout: Int, numberOfResponse: NatsRequest.NumberOfResponse) throws -> EventLoopFuture<NatsMessage>{
        guard let handler = handlerCacher.currentValue else {throw handlerError()}
        return try handler.streamingRequest(subject, payload:payload, timeout:timeout, numberOfResponse:numberOfResponse)
    }
    public func streamingSubscribe(_ subject: String, queueGroup: String = "", callback: @escaping ((_ T: NatsMessage) -> ())) throws -> EventLoopFuture<Void> {
        guard let handler = handlerCacher.currentValue else {throw handlerError()}
        return try handler.streamingSubscribe(subject, queueGroup: queueGroup, callback: callback)
    }
    
    public func subscribe(_ subject: String, queueGroup: String = "", callback: @escaping ((_ T: NatsMessage) -> ())) throws -> EventLoopFuture<Void>  {
        guard let handler = handlerCacher.currentValue else {throw handlerError()}
        return try handler.subscribe(subject, queueGroup: queueGroup, callback: callback)
    }
    
    public func unsubscribe(_ subject: String, max: UInt32 = 0) throws -> EventLoopFuture<Void> {
        guard let handler = handlerCacher.currentValue else {throw handlerError()}
        return try handler.unsubscribe(subject, max: max)
    }
    
    public func publish(_ subject: String, payload: Data) throws -> EventLoopFuture<Void> {
        guard let handler = handlerCacher.currentValue else {throw handlerError()}
        return try handler.publish(subject, payload: payload)
    }
    
    public func request(_ subject: String, payload: Data, timeout: Int, numberOfResponse: NatsRequest.NumberOfResponse) throws -> EventLoopFuture<NatsMessage> {
        guard let handler = handlerCacher.currentValue else {throw handlerError()}
        return try handler.request(subject, payload: payload, timeout: timeout, numberOfResponse: numberOfResponse)
    }
    public func publishRaw(payload: Data)throws -> EventLoopFuture<Void> {
        guard let handler = handlerCacher.currentValue, let ctx = handler.ctx else {throw handlerError()}
        return handler.write(ctx: ctx, data: payload)
    }
    func handlerError() -> NatsGeneralError {
        return NatsGeneralError(identifier: "Channel Handler not found", reason: "Internal Error, Channel handler not found for this thread")
    }
    
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
    var container: Container
    init(container: Container) {
        self.container = container
    }
}

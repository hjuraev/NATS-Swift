//
//  Client.swift
//  Run
//
//  Created by Halimjon Juraev on 2/2/18.
//

import Foundation
import TCP
import Logging
import Vapor
import Bits


public protocol NatsClientDelegate {
    func receivedMessage(container: Container, message: NatsMessage)
    func receivedRequest(container: Container, message: NatsMessage)
    func receivedError(container: Container, error: NatsError)
    func receivedPing(container: Container)
}

public struct NatsClientConfig: Codable, ServiceType {
    public static func makeService(for worker: Container) throws -> NatsClientConfig {
        return .default()
    }
    

    
    /// Default `RedisClientConfig`
    public static func `default`() -> NatsClientConfig {
        return .init(hostname: "localhost", port: 4222)
    }
    
    /// The Redis server's hostname.
    public var hostname: String
    
    /// The Redis server's port.
    public var port: UInt16
    
    /// Create a new `RedisClientConfig`
    public init(hostname: String, port: UInt16) {
        self.hostname = hostname
        self.port = port
    }
}

public final class NatsProvider: Provider {
    
    public static var repositoryName: String = "Nats Client"
    let config: NatsClientConfig
    
    init(config: NatsClientConfig) {
        self.config = config
    }
    public func register(_ services: inout Services) throws {
        services.register(NatsClient.self)
        services.register(config)
    }
    
    public func boot(_ worker: Container) throws {
    }
    
    
}


public final class NatsClient: ServiceType {
    
    public static func makeService(for worker: Container) throws -> NatsClient {
        let config: NatsClientConfig = try worker.make(NatsClientConfig.self, for: NatsClient.self)
        return try NatsClient(config: config, container: worker)
    }

    let client: TCPClient
    var StringData = String()
    var data: Data = Data()
    public let container: Container
    public var delegate: NatsClientDelegate?
    let parser: TranslatingStreamWrapper<NatsParser>
    fileprivate var subscriptions = [UUID:NatsSubscription]()
    fileprivate var server: Server?
    var requests:[String: Promise<NatsMessage>] = [:]
    
    
    public init(config: NatsClientConfig, container: Container) throws {
        self.container = container
        let socket = try TCPSocket(isNonBlocking: false)
        self.client = try TCPClient(socket: socket)
        try client.connect(hostname: config.hostname, port: config.port)
        var data = try client.socket.read(max: 3600)
        data.removeFirst(5)
        let decoder = JSONDecoder()
        decoder.dataDecodingStrategy = .deferredToData
        self.server = try decoder.decode(Server.self, from: data)
        self.parser = NatsParser(worker: container).stream(on: container)
    }
    

    public func listenSocket() {

        let stream = client.socket.source(on: container)
        _ = stream.stream(to: parser).drain { [weak self] message in
            guard let strongSelf = self else {return}
            switch message.proto {
            case .some(let value):
                switch value {
                case .PING:
                    strongSelf.processPing()
                    strongSelf.delegate?.receivedPing(container: strongSelf.container)
                case .CONNECT:
                    break
                case .SUB:
                    break
                case .UNSUB:
                    break
                case .PUB:
                    break
                case .MSG:
                    let localMessage = strongSelf.completeMessage(partial: message)
                    if localMessage.headers.replay != nil {
                        strongSelf.delegate?.receivedRequest(container: strongSelf.container, message: localMessage)
                    } else {
                        strongSelf.delegate?.receivedMessage(container: strongSelf.container, message: localMessage)
                    }
                    break
                case .INFO:
                    break
                case .OK:
                    break
                case .ERR:
                    let error = NatsError(description: String(data: message.rawValue, encoding: .utf8))
                    strongSelf.delegate?.receivedError(container: strongSelf.container, error: error)
                    break
                case .PONG:
                    break
                }
            case .none:
                break
            }


            }.catch { error in
                print(error.localizedDescription)
            }.finally {
                print("CLOSED STREAM")
        }

        
    }
    
    fileprivate func processPing() {
        print("Sending Pong")
        write(string: "\(Proto.PONG.rawValue)\r\n")
    }
    
    func completeMessage(partial: PartialNatsMessage) -> NatsMessage {
        var sub: NatsSubscription? = nil
        if let header = partial.headers, let uuid = UUID(uuidString: header.sid) {
            sub = subscriptions[uuid]
        }
        return NatsMessage(headers: partial.headers!, payload: partial.payload!, sub: sub)
    }
    
    private func write(string: String) {
        if let data = string.data(using: .utf8) {
            do {
               let result = try client.socket.write(data)
                switch result {
                case .success(count: let count):
                    if count == 0 {
                        /// SOCKET CLOSED NEED TO REOPEN
                    } else if count == data.count {
                        /// SUCCCESS
                    }
                case .wouldBlock:
                    break
                }
            } catch let error {
                print(error.localizedDescription)
            }
        }
    }
    

    open func subscribe(_ subject: String, queueGroup: String = "") -> Void {
        guard subscriptions.filter({ $0.value.subject == subject }).count == 0 else { return }
        let uuid = UUID()
        let sub = NatsSubscription(id: uuid, subject: subject, queueGroup: queueGroup, count: 0)
        subscriptions.updateValue(sub, forKey: uuid)
        write(string: sub.sub())
    }

    
    open func request(subject: String, reply: String) -> Future<NatsMessage> {
        let promise = Promise<NatsMessage>()
        let uuid = UUID()
        requests.updateValue(promise, forKey: uuid.uuidString)
        return promise.future
    }
    
    
    open func unsubscribe(_ subject: String, max: UInt32 = 0) {
        guard let sub = subscriptions.filter({ $0.value.subject == subject }).first else { return }
        subscriptions.removeValue(forKey: sub.key)
        write(string: sub.value.unsub(max))
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
        write(string: pub())
        
    }
    
    /**
     * reply(subject: String, replyto: String, payload: String)  -> Void
     * reply to id in subject
     *
     */
    open func reply(_ subject: String, replyto: String, payload: String) {
        let response: () -> String = {
            if let data = payload.data(using: String.Encoding.utf8) {
                return "\(Proto.PUB.rawValue) \(subject) \(replyto) \(data.count)\r\n\(payload)\r\n"
            }
            return ""
        }
        write(string: response())
    }
    
}

struct WriteError: Error {}

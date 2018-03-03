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
    func receivedMessage(message: NatsMessage)
    func receivedRequest(message: NatsMessage)
    func receivedError(error: NatsError)
    func receivedPing()
}



public final class NatsClient {
    
    let client: TCPClient
    var StringData = String()
    var data: Data = Data()
    var url: URI
    var worker: Worker
    public var delegate: NatsClientDelegate?
    let parser: TranslatingStreamWrapper<NatsParser>
    fileprivate var subscriptions = [UUID:NatsSubscription]()
    fileprivate var server: Server?
    var requests:[String: Promise<NatsMessage>] = [:]
    
    
    public init(hostname: String, port: UInt16, worker: Worker) throws {
        self.url = URI(scheme: nil, userInfo: nil, hostname: hostname, port: port, path: "/", query: nil, fragment: nil)
        self.worker = worker
        let socket = try TCPSocket(isNonBlocking: false)
        self.client = try TCPClient(socket: socket)
        try client.connect(hostname: hostname, port: port)
        var data = try client.socket.read(max: 3600)
        data.removeFirst(5)
        let decoder = JSONDecoder()
        decoder.dataDecodingStrategy = .deferredToData
        self.server = try decoder.decode(Server.self, from: data)
        self.parser = NatsParser(worker: worker).stream(on: worker)
    }
    

    public func listenSocket() {

        let stream = client.socket.source(on: worker)
        _ = stream.stream(to: parser).drain { [weak self] message in
            guard let strongSelf = self else {return}
            switch message.proto {
            case .some(let value):
                switch value {
                case .PING:
                    strongSelf.processPing()
                    strongSelf.delegate?.receivedPing()
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
                        strongSelf.delegate?.receivedRequest(message: localMessage)
                    } else {
                        strongSelf.delegate?.receivedMessage(message: localMessage)
                    }
                    break
                case .INFO:
                    break
                case .OK:
                    break
                case .ERR:
                    let error = NatsError(description: String(data: message.rawValue, encoding: .utf8))
                    strongSelf.delegate?.receivedError(error: error)
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

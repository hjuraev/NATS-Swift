//
//  NatsHandler.swift
//  NATS-Swift
//
//  Created by Halimjon Juraev on 3/22/18.
//

import Foundation
import NIO
import Async

class NatsHandler: ChannelInboundHandler {
    
    /// See `ChannelInboundHandler.InboundIn`
    public typealias InboundIn = NatsMessage
    
    /// See `ChannelInboundHandler.OutboundOut`
    public typealias OutboundOut = String
    
    /// Queue of input handlers and promises. Oldest (current) handler and promise are at the end of the array.
    private var inputQueue: [NatsMessage]
    
    /// Queue of output. Oldest objects are at the end of the array (output is dequeued with `popLast()`)
    private var outputQueue: [String]
    
    
    var currentCtx: ChannelHandlerContext?

    
    public var onNatsMessage: ((InboundIn) -> ())?
    public var onClose: (() -> ())?
    public var onOpen: (() -> ())?

    public var onSocketError: ((Error) -> ())?

    
    
    /// This handler's event loop.
    private let eventLoop: EventLoop
    
    /// A write-ready context waiting.
    
    /// Handles errors that happen when no input promise is waiting.
    private var errorHandler: (Error) -> ()
    
    /// Create a new `QueueHandler` on the supplied worker.
    public init(on worker: Worker, onError: @escaping (Error) -> ()) {
        self.inputQueue = []
        self.outputQueue = []
        self.eventLoop = worker.eventLoop
        self.errorHandler = onError
    }
    
    
    public func handlerAdded(ctx: ChannelHandlerContext) {
        self.currentCtx = ctx
    }
    
    
    func handlerRemoved(ctx: ChannelHandlerContext) {
        onClose?()
    }
    

    
    /// Enqueue new output to the handler.
    ///
    /// - parameters:
    ///     - output: An array of output (can be `0`) that you wish to send.
    ///     - onInput: A callback that will accept new input (usually responses to the output you enqueued)
    ///                The callback will continue to be called until you return `true` or an error is thrown.
    /// - returns: A future signal. Will be completed when `onInput` returns `true` or throws an error.
    public func enqueue(_ message: String) -> Bool {
        guard let ctx = currentCtx else {return false}
        
        guard ctx.channel.isActive else {
            // TODO: FIX DEBUG MESSAGE
            return false
        }
        outputQueue.insert(message, at: 0)
            ctx.eventLoop.execute {
                self.writeOutputIfEnqueued(ctx: ctx)
            }
        return true
    }
    
    /// Triggers a context write if any output is enqueued.
    private func writeOutputIfEnqueued(ctx: ChannelHandlerContext) {
        if let message = outputQueue.popLast() {
                ctx.write(wrapOutboundOut(message), promise: nil)
            ctx.flush()
            self.writeOutputIfEnqueued(ctx: ctx)
        }
    }
    
    /// MARK: ChannelInboundHandler conformance
    
    /// See `ChannelInboundHandler.channelRead(ctx:data:)`
    public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
        let msg = unwrapInboundIn(data) as NatsMessage
        onNatsMessage?(msg)
    }
    
    /// See `ChannelInboundHandler.channelActive(ctx:)`
    public func channelActive(ctx: ChannelHandlerContext) {
        writeOutputIfEnqueued(ctx: ctx)
        onOpen?()
    }
    
    /// See `ChannelInboundHandler.errorCaught(error:)`
    public func errorCaught(ctx: ChannelHandlerContext, error: Error) {
        onSocketError?(error)
    }
}


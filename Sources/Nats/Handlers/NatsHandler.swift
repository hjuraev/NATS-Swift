//
//  NatsHandler.swift
//  NATS-Swift
//
//  Created by Halimjon Juraev on 3/22/18.
//

import Foundation
import NIO
import Async

protocol NatsHandlerDelegate {
    func added(ctx: ChannelHandlerContext)
    func open(ctx: ChannelHandlerContext)
    func close(ctx: ChannelHandlerContext)
    func error(ctx: ChannelHandlerContext, error: Error)
    func message(ctx: ChannelHandlerContext, message: NatsMessages)
}


class NatsHandler: ChannelInboundHandler {
    
    /// See `ChannelInboundHandler.InboundIn`
    public typealias InboundIn = NatsMessages
    
    /// See `ChannelInboundHandler.OutboundOut`
    public typealias OutboundOut = Data
    
    var delegate: NatsHandlerDelegate?
    
    public var onNatsMessage: ((InboundIn) -> ())?
    
    public init() {
    }
    
    public func handlerAdded(ctx: ChannelHandlerContext) {
        debugPrint(ctx.handler)
        delegate?.added(ctx: ctx)
    }
    
    func handlerRemoved(ctx: ChannelHandlerContext) {
        delegate?.close(ctx: ctx)
    }
    
    /// Enqueue new output to the handler.
    ///
    /// - parameters:
    ///     - output: An array of output (can be `0`) that you wish to send.
    ///     - onInput: A callback that will accept new input (usually responses to the output you enqueued)
    ///                The callback will continue to be called until you return `true` or an error is thrown.
    /// - returns: A future signal. Will be completed when `onInput` returns `true` or throws an error.
    
    public func write(ctx:ChannelHandlerContext, data: Data) -> EventLoopFuture<Void>{
        return ctx.writeAndFlush(wrapOutboundOut(data))
    }
    
    /// See `ChannelInboundHandler.channelRead(ctx:data:)`
    public func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
        let msg = unwrapInboundIn(data) as NatsMessages
        delegate?.message(ctx: ctx, message: msg)
    }
    
    /// See `ChannelInboundHandler.channelActive(ctx:)`
    public func channelActive(ctx: ChannelHandlerContext) {
        delegate?.open(ctx: ctx)
    }
    
    /// See `ChannelInboundHandler.errorCaught(error:)`
    public func errorCaught(ctx: ChannelHandlerContext, error: Error) {
        delegate?.error(ctx: ctx, error: error)
    }
}

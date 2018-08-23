//
//  NatsEncoder.swift
//  Async
//
//  Created by Halimjon Juraev on 3/22/18.
//

import Foundation
import NIO
import Bits

class NatsEncoder: MessageToByteEncoder{
    typealias OutboundIn = Data
    
    
    func encode(ctx: ChannelHandlerContext, data: NatsEncoder.OutboundIn, out: inout ByteBuffer) throws {
        out.write(bytes: data)
    }
    
}

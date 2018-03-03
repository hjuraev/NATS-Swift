import Async
import Foundation
import COperatingSystem
import Bits

final class NatsParser: ByteParser {
    
    
    func parseBytes(from buffer: ByteBuffer, partial: NatsParser.PartialFrame?) throws -> Future<ByteParserResult<NatsParser>> {
        return try Future(_parseBytes(from: buffer, partial: partial))
    }
    
    private func _parseBytes(from buffer: ByteBuffer, partial: NatsParser.PartialFrame?) throws -> ByteParserResult<NatsParser> {
        
        if let partial = partial {
            return try self.continueParsing(partial, from: buffer)
        } else {
            return try self.startParsing(from: buffer)
        }
    }
    
    enum PartialFrame {
        case partial(PartialNatsMessage)
    }
    
    /// See InputStream.Input
    public typealias Input = ByteBuffer
    
    /// See OutputStream.Output
    public typealias Output = PartialNatsMessage
    
    typealias Partial = PartialFrame
    
    var bufferBuilder: MutableBytesPointer
    
    var accumulated = 0
    
    var state: ByteParserState<NatsParser>
    
    /// The maximum accepted payload size (to prevent memory attacks)
    let maximumPayloadSize: Int
    
    init(maximumPayloadSize: UInt64 = 100_000, worker: Worker) {
        assert(maximumPayloadSize < Int.max - 15, "The maximum WebSocket payload size is too large")
        assert(maximumPayloadSize > 0, "The maximum WebSocket payload size is negative or 0")
        
        self.maximumPayloadSize = numericCast(maximumPayloadSize)
        self.state = .init()
        
        // 2 for the header, 9 for the length, 4 for the mask
        self.bufferBuilder = MutableBytesPointer.allocate(capacity: 15 + self.maximumPayloadSize)
    }
    
    
    
    func startParsing(from buffer: ByteBuffer) throws -> ByteParserResult<NatsParser> {

        
        
        let message = try parseFrameHeader(from: buffer)
            if message.completed {
                return .completed(consuming: message.consumed + 1, result: message)
            } else {
                _ = MutableByteBuffer(start: bufferBuilder.advanced(by: message.consumed + 1), count: buffer.count).initialize(from: buffer)
                return .uncompleted(.partial(message))
            }
    }
    

    
    func continueParsing(_ partial: PartialFrame, from buffer: ByteBuffer) throws -> ByteParserResult<NatsParser> {

        switch partial {
        case .partial(let message):

 
            let message = try parseFrameHeader(partial: message, from: buffer)
            
            if message.completed {
                defer {
                    // Always write these. Ensures that successful and uncompleted parsing are covered, always
                    _ = MutableByteBuffer(start: bufferBuilder, count: message.consumed + 1).initialize(from: buffer)
                }
                return .completed(consuming: message.consumed + 1, result: message)
            } else {
                defer {
                    // Always write these. Ensures that successful and uncompleted parsing are covered, always
                    _ = MutableByteBuffer(start: bufferBuilder, count: buffer.count).initialize(from: buffer)
                }
                return .uncompleted(.partial(message))
            }

        }
        
    }
    

    func parseFrameHeader(partial: PartialNatsMessage? = nil, from buffer: ByteBuffer) throws -> PartialNatsMessage {
        
        
        var message: PartialNatsMessage = PartialNatsMessage()
        
        if let partial = partial {
            message = partial
        }
        
        var localErrorSting = Data()
        var localMSGArg: [Byte] = []
        
        for (x, value) in buffer.enumerated() {
            message.rawValue.append(value)
            message.consumed = x
            switch message.state {
            case .OP_START:
                switch value {
                case .M, .m:
                    message.state = .OP_M
                case .P, .p:
                    message.state = .OP_P
                case .plus:
                    message.state = .OP_PLUS
                case .hyphen:
                    message.state = .OP_MINUS
                case .I, .i:
                    message.state = .OP_I
                    
                default:
                    messageFatal()
                    // ERROR
                    break
                }
            case .OP_PLUS:
                switch value {
                case .O, .o:
                    message.state = .OP_PLUS_O
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_PLUS_O:
                switch value {
                case .K, .k:
                    message.state = .OP_PLUS_OK
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_PLUS_OK:
                switch value {
                case .newLine:
                    /// PROCESS OK
                    message.proto = .OK
                    message.state = .OP_START
                    message.completed = true

                    return message
                default:
                    break
                }
            case .OP_MINUS:
                switch value {
                case .E, .e:
                    message.state = .OP_MINUS_E
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_MINUS_E:
                switch value {
                case .R, .r:
                    message.state = .OP_MINUS_ER
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_MINUS_ER:
                switch value {
                case .R, .r:
                    message.state = .OP_MINUS_ERR
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_MINUS_ERR:
                switch value {
                case .space, .horizontalTab:
                    message.state = .OP_MINUS_ERR_SPC
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_MINUS_ERR_SPC:
                
                switch value {
                case .space, .horizontalTab:
                    continue
                default:
                    message.state = .MINUS_ERR_ARG
                    // ERROR
                    break
                }
            case .MINUS_ERR_ARG:
                switch value {
                case .carriageReturn:
                    
                    break
                case .newLine:
                    message.state = .OP_START
                    message.proto = .ERR
                    message.info = localErrorSting
                    message.completed = true
                    return message
                default:
                    localErrorSting.append(value)
                    // TODO: PROCESS ERROR MESSAGE FURTHER
                    break
                }
            case .OP_M:
                switch value {
                case .S, .s:
                    message.state = .OP_MS
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_MS:
                switch value {
                case .G, .g:
                    message.state = .OP_MSG
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_MSG:
                switch value {
                case .space, .horizontalTab:
                    message.state = .OP_MSG_SPC
                default:
                    messageFatal()

                    // ERROR
                    break
                }
            case .OP_MSG_SPC:
                switch value {
                case .space, .horizontalTab:
                    continue
                default:
                    message.state = .MSG_ARG
                    localMSGArg.append(value)
                    break
                }
            case .MSG_ARG:
                switch value {
                case .carriageReturn:
                    break
                case .newLine:
                    message.state = .MSG_PAYLOAD
                    message.headers = self.generateHeaders(data: localMSGArg)
                default:
                    localMSGArg.append(value)
                    break
                }
            case .MSG_PAYLOAD:
                if message.headers != nil, message.headers?.size == message.payload?.count {
                        message.state = .MSG_END
                } else {
                    if message.payload == nil {
                        message.payload = Data()
                        message.payload?.append(value)
                    } else {
                        message.payload?.append(value)
                    }
                }
                
            case .MSG_END:
                switch value {
                case .newLine:
                    message.proto = .MSG
                    message.completed = true

                    return message
                default:
                    continue
                }
            case .OP_P:
                switch value {
                case .I, .i:
                    message.state = .OP_PI
                case .O, .o:
                    message.state = .OP_PO
                default:
                    break
                }
            case .OP_PI:
                switch value {
                case .N, .n:
                    message.state = .OP_PIN
                default:
                    break
                }
            case .OP_PIN:
                switch value {
                case .G, .g:
                    message.state = .OP_PING
                default:
                    break
                }
            case .OP_PING:
                switch value {
                case .newLine:
                    /// TODO: PROCESS PING
                    message.proto = .PING
                    message.completed = true
                    message.state = .OP_START
                    return message
                default:
                    break
                }
            case .OP_PO:
                switch value {
                case .N, .n:
                    message.state = .OP_PON
                default:
                    break
                }
            case .OP_PON:
                switch value {
                case .G, .g:
                    message.state = .OP_PONG
                default:
                    break
                }
            case .OP_PONG:
                switch value {
                case .newLine:
                    // TODO: PROCESS PONG
                    message.proto = .PING
                    message.completed = true
                    message.state = .OP_START
                    return message
                default:
                    break
                }
            case .OP_I:
                switch value {
                case .N, .n:
                    message.state = .OP_IN
                default:
                    break
                }
            case .OP_IN:
                switch value {
                case .F, .f:
                    message.state = .OP_INF
                default:
                    break
                }
            case .OP_INF:
                switch value {
                case .O, .o:
                    message.state = .OP_INFO
                default:
                    break
                }
            case .OP_INFO:
                switch value {
                case .space, .horizontalTab:
                    message.state = .OP_INFO_SPC
                default:
                    break
                }
            case .OP_INFO_SPC:
                switch value {
                case .space, .horizontalTab:
                    continue
                default:
                    message.state = .INFO_ARG
                    break
                }
            case .INFO_ARG:
                switch value {
                case .carriageReturn:
                    break
                case .newLine:
                    message.proto = .INFO
                    message.completed = true
                    message.state = .OP_START

                    return message
                default:
                    if message.info == nil {
                        message.info = Data()
                        message.info?.append(value)
                    } else {
                        message.info?.append(value)
                    }
                    break
                }
            }
        }
        return message
    }
    
    
    func generateHeaders(data: Bytes) -> NatsMessage.Headers? {
        if let stringRepresentation = String(bytes: data, encoding: .utf8) {
            let components = stringRepresentation.components(separatedBy: .whitespacesAndNewlines)
                switch components.count {
                case 3:
                    let header = NatsMessage.Headers(subject: components[0], sid: components[1], replay: nil, size: Int(components[2]) ?? 0)
                    return header
                case 4:
                    let header = NatsMessage.Headers(subject: components[0], sid: components[1], replay: components[2], size: Int(components[3]) ?? 0)
                    return header
                default:
                    /// error parsing headers
                    break
                }
        }
        return nil
    }
    
    deinit {
        bufferBuilder.deallocate()
    }
    
    
    
    enum MESSAGE_STATE {
        case OP_START
        case OP_PLUS
        case OP_PLUS_O
        case OP_PLUS_OK
        case OP_MINUS
        case OP_MINUS_E
        case OP_MINUS_ER
        case OP_MINUS_ERR
        case OP_MINUS_ERR_SPC
        case MINUS_ERR_ARG
        case OP_M
        case OP_MS
        case OP_MSG
        case OP_MSG_SPC
        case MSG_ARG
        case MSG_PAYLOAD
        case MSG_END
        case OP_P
        case OP_PI
        case OP_PIN
        case OP_PING
        case OP_PO
        case OP_PON
        case OP_PONG
        case OP_I
        case OP_IN
        case OP_INF
        case OP_INFO
        case OP_INFO_SPC
        case INFO_ARG
    }
    
    func messageFatal(){
        fatalError()
    }
    
    

}

/// Various states the parser stream can be in
enum ProtocolParserState {
    /// normal state
    case ready
    
    /// waiting for data from upstream
    case awaitingUpstream
}

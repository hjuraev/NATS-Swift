import Async
import Foundation
import Bits
import NIO
import Vapor




class NatsDecoder: ByteToMessageDecoder {
    
    
    public typealias InboundIn = ByteBuffer
    public typealias InboundOut = NatsMessage
    public typealias OutboundOut = NatsMessage
    public var cumulationBuffer: ByteBuffer? = nil
    
    var parser = NatsParser()
    private var shouldKeepParsing = true

    
    func decode(ctx: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        
        
        parseLoop: while self.shouldKeepParsing {
            switch parser.parse(buffer: &buffer){
            case .result(let message):
                ctx.fireChannelRead(self.wrapInboundOut(message))
            case .continueParsing:
                break
            case .insufficientData:
                break parseLoop
            }
        }
        // We parse eagerly, so once we get here we definitionally need more data.
        return .needMoreData
    }
}


struct NatsParser {
    var rawValue: Data = Data()
    var proto: Proto?
    var headers: MSG.Headers?
    var payload: Data = Data()
    var state: MESSAGE_STATE = .OP_START
    var info: Data = Data()
    var consumed: Int = 0
    var localErrorSting = Data()
    var localMSGArg: [Byte] = []
    
    private mutating func reset() {
        self.rawValue = Data()
        self.proto = nil
        self.headers = nil
        self.payload = Data()
        self.state = .OP_START
        self.info = Data()
        self.consumed = 0
        self.localErrorSting = Data()
        self.localMSGArg = []
    }
    
    
    
    mutating func parse(buffer: inout ByteBuffer) -> ParseResult {
        guard let value = buffer.readBytes(length: 1)?.first else {return .insufficientData}
        rawValue.append(contentsOf: [value])
        switch state {
        case .OP_START:
            
            switch value {
            case .M, .m:
                state = .OP_M
            case .P, .p:
                state = .OP_P
            case .plus:
                state = .OP_PLUS
            case .hyphen:
                state = .OP_MINUS
            case .I, .i:
                state = .OP_I
            default:
                messageFatal()

                // ERROR
                break
            }
        case .OP_PLUS:
            switch value {
            case .O, .o:
                state = .OP_PLUS_O
            default:
                messageFatal()
                
                // ERROR
                break
            }
        case .OP_PLUS_O:
            switch value {
            case .K, .k:
                state = .OP_PLUS_OK
            default:
                messageFatal()
                
                // ERROR
                break
            }
        case .OP_PLUS_OK:
            switch value {
            case .newLine:
                /// PROCESS OK
                proto = .OK
                state = .OP_START
                reset()
                return .result(.OK)

            default:
                break
            }
        case .OP_MINUS:
            switch value {
            case .E, .e:
                state = .OP_MINUS_E
            default:
                messageFatal()
                
                // ERROR
                break
            }
        case .OP_MINUS_E:
            switch value {
            case .R, .r:
                state = .OP_MINUS_ER
            default:
                messageFatal()
                
                // ERROR
                break
            }
        case .OP_MINUS_ER:
            switch value {
            case .R, .r:
                state = .OP_MINUS_ERR
            default:
                messageFatal()
                
                // ERROR
                break
            }
        case .OP_MINUS_ERR:
            switch value {
            case .space, .horizontalTab:
                state = .OP_MINUS_ERR_SPC
            default:
                messageFatal()
                
                // ERROR
                break
            }
        case .OP_MINUS_ERR_SPC:
            
            switch value {
            case .space, .horizontalTab:
                break
            default:
                state = .MINUS_ERR_ARG
                // ERROR
                break
            }
        case .MINUS_ERR_ARG:
            switch value {
            case .carriageReturn:
                
                break
            case .newLine:
                state = .OP_START
                proto = .ERR
                reset()
                let error = NatsError(description: String(bytes: localErrorSting, encoding: .utf8))
                return .result(.ERR(error))
            default:
                localErrorSting.append(value)
                // TODO: PROCESS ERROR MESSAGE FURTHER
                break
            }
        case .OP_M:
            switch value {
            case .S, .s:
                state = .OP_MS
            default:
                messageFatal()
                // ERROR
                break
            }
        case .OP_MS:
            switch value {
            case .G, .g:
                state = .OP_MSG
            default:
                messageFatal()
                // ERROR
                break
            }
        case .OP_MSG:
            switch value {
            case .space, .horizontalTab:
                state = .OP_MSG_SPC
            default:
                messageFatal()
                
                // ERROR
                break
            }
        case .OP_MSG_SPC:
            switch value {
            case .space, .horizontalTab:
                break
            default:
                state = .MSG_ARG
                localMSGArg.append(value)
                break
            }
        case .MSG_ARG:
            switch value {
            case .carriageReturn:
                break
            case .newLine:
                state = .MSG_PAYLOAD
                headers = self.generateHeaders(data: localMSGArg)
            default:
                localMSGArg.append(value)
                break
            }
        case .MSG_PAYLOAD:
            if headers != nil, headers?.size == payload.count {
                state = .MSG_END
            } else {
                payload.append(value)
            }
        case .MSG_END:
            switch value {
            case .newLine:
                proto = .MSG
                if let completedMessage = completeMessage() {
                    reset()
                    return .result(.MSG(completedMessage))
                } else {
                    fatalError()
                }
                /// MESSAGE COMPLETED
            default:
                break
            }
        case .OP_P:
            switch value {
            case .I, .i:
                state = .OP_PI
            case .O, .o:
                state = .OP_PO
            default:
                break
            }
        case .OP_PI:
            switch value {
            case .N, .n:
                state = .OP_PIN
            default:
                break
            }
        case .OP_PIN:
            switch value {
            case .G, .g:
                state = .OP_PING
            default:
                break
            }
        case .OP_PING:
            switch value {
            case .newLine:
                proto = .PING
                state = .OP_START
                reset()
                return .result(.PING)
                /// PING RECEIVED
            default:
                break
            }
        case .OP_PO:
            switch value {
            case .N, .n:
                state = .OP_PON
            default:
                break
            }
        case .OP_PON:
            switch value {
            case .G, .g:
                state = .OP_PONG
            default:
                break
            }
        case .OP_PONG:
            switch value {
            case .newLine:
                proto = .PING
                state = .OP_START
                reset()
                return .result(.PONG)
            default:
                break
            }
        case .OP_I:
            switch value {
            case .N, .n:
                state = .OP_IN
            default:
                break
            }
        case .OP_IN:
            switch value {
            case .F, .f:
                state = .OP_INF
            default:
                break
            }
        case .OP_INF:
            switch value {
            case .O, .o:
                state = .OP_INFO
            default:
                break
            }
        case .OP_INFO:
            switch value {
            case .space, .horizontalTab:
                state = .OP_INFO_SPC
            default:
                break
            }
        case .OP_INFO_SPC:
            switch value {
            case .space, .horizontalTab:

                break
            default:
                state = .INFO_ARG

                break
            }
        case .INFO_ARG:
            switch value {
            case .carriageReturn:
                break
            case .newLine:
                proto = .INFO
                state = .OP_START
                    do {
                        let newServer: Data = rawValue.advanced(by: 5)
                        let decoder = JSONDecoder()
                        let server = try decoder.decode(Server.self, from: newServer)
                        reset()
                        return .result(.INFO(server))
                    }catch let error {
                        debugPrint(error)
                }
            default:
                
                break
            }
        }
        return .continueParsing
    }
    
    func completeMessage() -> MSG? {
        guard let localHeaders = headers else {return nil}
        let message = MSG(headers: localHeaders, payload: payload, sub: nil)
        return message
    }
    
    func generateHeaders(data: Bytes) -> MSG.Headers? {
        if let stringRepresentation = String(bytes: data, encoding: .utf8) {
            let components = stringRepresentation.components(separatedBy: .whitespacesAndNewlines)
            switch components.count {
            case 3:
                if let uuid = UUID(uuidString: components[1]) {
                    let header = MSG.Headers(subject: components[0], sid: uuid, replay: nil, size: Int(components[2]) ?? 0)
                    return header
                }
            case 4:
                if let uuid = UUID(uuidString: components[1]) {
                    let header = MSG.Headers(subject: components[0], sid: uuid, replay: components[2], size: Int(components[3]) ?? 0)
                    return header
                }
            default:
                /// error parsing headers
                break
            }
        }
        return nil
    }
    
    func messageFatal(){
        // TODO: FATAL ERROR IN PROTOCOL
        fatalError("ERROR PARSING NAT MESSAGE")
    }
    
    enum ParseResult {
        case insufficientData
        case continueParsing
        case result(NatsMessage)
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
    
}



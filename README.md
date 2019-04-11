# NATS Client implemented in swift

NATS is an open-source, cloud-native messaging system. Companies like Apcera, Baidu, Siemens, VMware, HTC, and Ericsson rely on NATS for its highly performant and resilient messaging capabilities.

Its Nats Client/Streaming implemented in swift 

Build for Vapor 3 depends on SwiftProto and Swift NIO

If you have ideas to improve any part of the code please contact me

Configuration file: 
```swift
    services.register(NatsRouter.self) { _ -> (RouterController) in
        return RouterController()
    }
    
    services.register { container -> (NATS) in
        let servers = [natsServer(hostname: "0.0.0.0")]
        
        let natsConfig = NatsClientConfig(servers: servers, connectionType: .server(.multiple(1)), disBehavior: .reconnect, clusterName: "_STAN.discover.test-cluster", streaming: true)
        return try NATS(container: container, config: natsConfig)
    }
```


Note: 

Connection type 
1. Server with multiple eventloops (Each eventloop creates separate connection to Nats server)
2. Client(Creates connection if it does not have one for current eventloop)


RouterController file: 
```swift
class RouterController: Service, NatsRouter {
    func onOpen(handler: NatsHandler, ctx: ChannelHandlerContext) {
        print("OPEN")
    }
    
    func onStreamingOpen(handler: NatsHandler, ctx: ChannelHandlerContext) {
        print("STREAM OPEN")
    }
    
    func onClose(handler: NatsHandler, ctx: ChannelHandlerContext) {
        print("CLOSED")
    }
    
    func onError(handler: NatsHandler, ctx: ChannelHandlerContext, error: Error) {
        print("ERROR -> \(error.localizedDescription)")
    }
}
```

Subscription:
```swift
    handler.subscribe("Subject") { (msg) in
            
    }
```

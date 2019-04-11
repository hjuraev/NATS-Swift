import Foundation
import Nats
import NIO
//import NIOScyllaDB
//import Foundation
//
let eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()


//let conn = try PostgresConnection.connect(to: .init(ipAddress: "10.7.8.201", port: 9042), on: eventLoop).wait()
//try conn.authenticate(username: "cassandra", password: "cassandra").wait()
//print("Starting 10k CQL requests in 1 thread")
//print("SELECT version();")
//
//let startTime = Date()
//for _ in 0..<10_000 {
//    do {
//        _ = try conn.simpleQuery("SELECT * from system_schema.columns;").wait()
//
//    } catch {
//        debugPrint(error)
//    }
//}
//let endtime = Date()
//
//
//print("Completed in: \(endtime.timeIntervalSince(startTime)) seconds")
//
//print("Done")
//try conn.close().wait()

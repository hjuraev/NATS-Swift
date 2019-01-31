//
//  StreamingConnect.swift
//  Nats
//
//  Created by Halimjon Juraev on 12/27/18.
//

import Foundation


struct ConnectRequest {
    func getStringValue() -> String {
        return "\(clientID.uuidString) \(heartbeatInbox)\r\n"
    }
    
    let clientID: UUID
    let heartbeatInbox: String
    
    init(clientID: UUID, heartbeatInbox: String) {
        self.clientID = clientID
        self.heartbeatInbox = heartbeatInbox
    }
}

struct ConnectResponse {
    let pubPrefix: String
    let subRequests: String
    let unsubRequests: String
    let closeRequests: String
    let error: String?
    let publicKey: String? // NOT YET IMPLEMENTED BY NATS
}

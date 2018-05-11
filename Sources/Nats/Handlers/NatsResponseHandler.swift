//
//  RequestHandler.swift
//  App
//
//  Created by Halimjon Juraev on 5/9/18.
//

import Foundation
import Vapor

public class NatsResponseStorage: Service {
    var subscriptions = [UUID:NatsSubscription]()
    var requestsStorage = [UUID:NatsRequest]()

    public init() {}
}

﻿namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pulsar.Client.Common
open System
open System.Net
open System.Threading.Tasks
open Microsoft.Extensions.Logging


type BinaryLookupService (config: PulsarClientConfiguration, connectionPool: ConnectionPool) =

    let endpoint = DnsEndPoint(config.ServiceUrl.Host, config.ServiceUrl.Port)

    member this.GetPartitionsForTopic (topicName: TopicName) =
        task {
            let! metadata = this.GetPartitionedTopicMetadata topicName.CompleteTopicName
            if metadata.Partitions > 0
            then
                return Array.init metadata.Partitions (fun i -> topicName.GetPartition(i))
            else
                return [| topicName |]
        }

    member this.GetPartitionedTopicMetadata topicName =
        task {
            let! clientCnx = connectionPool.GetBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = Commands.newPartitionMetadataRequest topicName requestId
            let! response = clientCnx.SendAndWaitForReply requestId payload
            return PulsarResponseType.GetPartitionedTopicMetadata response
        }

    member this.GetBroker(topicName: CompleteTopicName) =
        this.FindBroker(endpoint, false, topicName)

    member private this.FindBroker(endpoint: DnsEndPoint, autoritative: bool, topicName: CompleteTopicName) =
        task {
            let! clientCnx = connectionPool.GetBrokerlessConnection endpoint
            let requestId = Generators.getNextRequestId()
            let payload = Commands.newLookup topicName requestId autoritative
            let! response = clientCnx.SendAndWaitForReply requestId payload
            let lookupTopicResult = PulsarResponseType.GetLookupTopicResult response
            // (1) build response broker-address
            let uri =
                if config.UseTls then
                    Uri(lookupTopicResult.BrokerServiceUrlTls)
                else
                    Uri(lookupTopicResult.BrokerServiceUrl)

            let resultEndpoint = DnsEndPoint(uri.Host, uri.Port)
            // (2) redirect to given address if response is: redirect
            if lookupTopicResult.Redirect then
                Log.Logger.LogDebug("Redirecting to {0} topicName {1}", resultEndpoint, topicName)
                return!  this.FindBroker(resultEndpoint, lookupTopicResult.Authoritative, topicName)
            else
                // (3) received correct broker to connect
                return if lookupTopicResult.Proxy
                       then { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress endpoint }
                       else { LogicalAddress = LogicalAddress resultEndpoint; PhysicalAddress = PhysicalAddress resultEndpoint }
        }

    member this.GetTopicsUnderNamespace (ns : NamespaceName, mode : TopicDomain) =
        task {
            let backoff = Backoff { BackoffConfig.Default with
                                        Initial = TimeSpan.FromMilliseconds(100.0)
                                        MandatoryStop = (config.OperationTimeout + config.OperationTimeout) }
            let! result = this.GetTopicsUnderNamespace(endpoint, ns, backoff, int config.OperationTimeout.TotalMilliseconds, mode)
            return result
        }

    member private this.GetTopicsUnderNamespace (endpoint: DnsEndPoint, ns: NamespaceName, backoff: Backoff, remainingTimeMs: int, mode: TopicDomain) =
        async {
            try
                let! clientCnx = connectionPool.GetBrokerlessConnection endpoint |> Async.AwaitTask
                let requestId = Generators.getNextRequestId()
                let payload = Commands.newGetTopicsOfNamespaceRequest ns requestId mode
                let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                return (endpoint, response)
            with ex ->
                let delay = Math.Min(backoff.Next(), remainingTimeMs)
                if delay <= 0 then
                    raise (TimeoutException "Could not getTopicsUnderNamespace within configured timeout.")
                Log.Logger.LogWarning(ex, "GetTopicsUnderNamespace failed will retry in {0} ms", delay)
                do! Async.Sleep delay
                return! this.GetTopicsUnderNamespace(endpoint, ns, backoff, remainingTimeMs - delay, mode)
        }
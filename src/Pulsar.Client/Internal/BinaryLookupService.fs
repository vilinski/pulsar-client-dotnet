﻿namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open Pipelines.Sockets.Unofficial
open System.Buffers
open System.Text
open Pulsar.Client.Common
open System
open Utf8Json
open System.Threading.Tasks


type BinaryLookupService (config: PulsarClientConfiguration) =
    let serviceNameResolver = ServiceNameResolver(config)

    member __.GetPartitionedTopicMetadata topicName = 
        task {
            let endpoint = serviceNameResolver.ResolveHost()
            use! conn = SocketConnection.ConnectAsync(endpoint)
            let requestId = Generators.getNextRequestId()
            let request = 
                Commands.newPartitionMetadataRequest topicName requestId
                |> ReadOnlyMemory<byte>
            let! flushResult = conn.Output.WriteAsync(request)
            conn.Output.Complete()
            let mutable continueLooping = true
            let mutable partitionedTopicMetadata = {Partitions= 0u}
            while continueLooping do
                let! result = conn.Input.ReadAsync()
                let buffer = result.Buffer
                if result.IsCompleted
                then
                    let array = result.Buffer.ToArray()
                    partitionedTopicMetadata <- PulsarDecoder.handlePartitionResponse array
                    continueLooping <- false
                else
                    conn.Input.AdvanceTo(buffer.Start, buffer.End)
            return partitionedTopicMetadata
        }

    member __.GetServiceUrl(): string = 
        raise (System.NotImplementedException())

    member __.UpdateServiceUrl(serviceUrl): unit = 
        raise (System.NotImplementedException())

    member __.GetBroker(topicName: TopicName): Task<Broker> = 
        raise (System.NotImplementedException())
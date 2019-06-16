module Pulsar.Client.Common.PulsarDecoder
open System.IO
open ProtoBuf
open Pulsar.Client.Common
open pulsar.proto


let deserializeCommand (data:byte[]) =
    use ms = new MemoryStream(data)
    Serializer.Deserialize<BaseCommand>(ms)

let handlePartitionResponse data =
    let cmd = deserializeCommand data
    let lookupResult = cmd.partitionMetadataResponse
    let requestId = lookupResult.RequestId
    //todo check requestId    
    {Partitions = lookupResult.Partitions}
﻿namespace Pulsar.Client.Api

open Pulsar.Client.Common

type ProducerBuilder private (client: PulsarClient, config: ProducerConfiguration) =

    let verify(config : ProducerConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> throwIfDefault (fun() ->  ProducerException("Topic name must be set on the producer builder.")))

    new(client: PulsarClient) = ProducerBuilder(client, ProducerConfiguration.Default)

    member __.Topic topic =
        ProducerBuilder(
            client,
            { config with
                Topic = topic
                    |> invalidArgIfBlankString "Topic must not be blank."
                    |> TopicName })

    member __.ProducerName producerName =
        ProducerBuilder(
            client,
            { config with
                ProducerName = producerName |> invalidArgIfBlankString "ProducerName must not be blank." })

    member __.MaxPendingMessages maxPendingMessages =
        ProducerBuilder(
            client,
            { config with
                MaxPendingMessages =
                    maxPendingMessages
                    |> invalidArgIfNotGreaterThanZero "MaxPendingMessages needs to be greater than 0." })

    member __.EnableBatching batchMessagesEnabled =
        ProducerBuilder(
            client,
            { config with BatchingEnabled = batchMessagesEnabled })

    member __.BatchingMaxMessages maxMessagesPerBatch =
        ProducerBuilder(
            client,
            { config with
                MaxMessagesPerBatch =
                    maxMessagesPerBatch
                    |> invalidArgIfLessThanZero "BatchingMaxMessages needs to be non negative integer." })

    member __.BatchingMaxPublishDelay batchingMaxPublishDelay =
        ProducerBuilder(
            client,
            { config with MaxBatchingPublishDelay = batchingMaxPublishDelay })

    member __.CreateAsync() =
        config
        |> verify
        |> client.CreateProducerAsync
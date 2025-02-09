﻿namespace Pulsar.Client.Api

open Pulsar.Client.Common

type ReaderBuilder private (client: PulsarClient, config: ReaderConfiguration) =

    let verify(config : ReaderConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the reader builder.")
        |> checkValue
            (fun c ->
                c.StartMessageId
                |> invalidArgIfDefault "StartMessageId name name must be set on the reader builder.")

    new(client: PulsarClient) = ReaderBuilder(client, ReaderConfiguration.Default)

    member this.Topic (topic: string) =
        ReaderBuilder(
            client,
            { config with
                Topic = topic
                    |> invalidArgIfBlankString "Topic must not be blank."
                    |> TopicName })

    member this.StartMessageId (messageId: MessageId) =
        ReaderBuilder(
            client,
            { config with
                StartMessageId = messageId
                    |> invalidArgIfDefault "Topic must not be blank." })

    member this.StartMessageIdInclusive (startMessageIdInclusive: bool) =
        ReaderBuilder(
            client,
            { config with
                ResetIncludeHead = startMessageIdInclusive })

    member this.ReadCompacted readCompacted =
        ReaderBuilder(
            client,
            { config with
                ReadCompacted = readCompacted })

    member this.SubscriptionRolePrefix subscriptionRolePrefix =
        ReaderBuilder(
            client,
            { config with
                SubscriptionRolePrefix = subscriptionRolePrefix })

    member this.ReaderName readerName =
        ReaderBuilder(
            client,
            { config with
                ReaderName = readerName |> invalidArgIfBlankString "ReaderName must not be blank." })

    member this.ReceiverQueueSize receiverQueueSize =
        ReaderBuilder(
            client,
            { config with
                ReceiverQueueSize = receiverQueueSize |> invalidArgIfNotGreaterThanZero "ReceiverQueueSize should be greater than 0."  })

    member this.CreateAsync() =
        config
        |> verify
        |> client.CreateReaderAsync


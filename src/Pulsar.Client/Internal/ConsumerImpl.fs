﻿namespace Pulsar.Client.Api

open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks
open FSharp.UMX
open System.Collections.Generic
open System
open Pulsar.Client.Internal
open Pulsar.Client.Common
open Microsoft.Extensions.Logging
open System.IO
open ProtoBuf
open pulsar.proto
open System.Collections.Generic
open System.Linq

type ConsumerState = {
    WaitingChannel: AsyncReplyChannel<MessageOrException>
}

type ConsumerImpl internal (consumerConfig: ConsumerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                           partitionIndex: int, subscriptionMode: SubscriptionMode, startMessageId: MessageId option, lookup: BinaryLookupService,
                           cleanup: ConsumerImpl -> unit) as this =

    [<Literal>]
    let MAX_REDELIVER_UNACKNOWLEDGED = 1000
    let consumerId = Generators.getNextConsumerId()
    let incomingMessages = new Queue<Message>()
    let nullChannel = Unchecked.defaultof<AsyncReplyChannel<MessageOrException>>
    let subscribeTsc = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let prefix = sprintf "consumer(%u, %s, %i)" %consumerId consumerConfig.ConsumerName partitionIndex
    let subscribeTimeout = DateTime.Now.Add(clientConfig.OperationTimeout)
    let mutable hasReachedEndOfTopic = false
    let mutable avalablePermits = 0
    let mutable lastMessageIdInBroker = MessageId.Earliest
    let mutable lastDequeuedMessage = MessageId.Earliest
    let mutable startMessageId = startMessageId
    let receiverQueueRefillThreshold = consumerConfig.ReceiverQueueSize / 2
    let connectionHandler =
        ConnectionHandler(prefix,
                          connectionPool,
                          lookup,
                          consumerConfig.Topic.CompleteTopicName,
                          (fun () -> this.Mb.Post(ConsumerMessage.ConnectionOpened)),
                          (fun ex -> this.Mb.Post(ConsumerMessage.ConnectionFailed ex)),
                          Backoff({ BackoffConfig.Default with Initial = TimeSpan.FromMilliseconds(100.0); Max = TimeSpan.FromSeconds(60.0) }))

    let hasMoreMessages (lastMessageIdInBroker: MessageId) (lastDequeuedMessage: MessageId) =
        if (lastMessageIdInBroker > lastDequeuedMessage && lastMessageIdInBroker.EntryId <> %(-1L)) then
            true
        else
            lastMessageIdInBroker = lastDequeuedMessage && incomingMessages.Count > 0

    let increaseAvailablePermits delta =
        avalablePermits <- avalablePermits + delta
        if avalablePermits >= receiverQueueRefillThreshold then
            this.Mb.Post(ConsumerMessage.SendFlowPermits avalablePermits)
            avalablePermits <- 0

    let getStartMessageId() =
        if incomingMessages.Count > 0 then
            let lastMessage = incomingMessages.Last()
            lastMessage.MessageId
        else if lastDequeuedMessage <> MessageId.Earliest then
            lastDequeuedMessage
        else
            match startMessageId with
            | Some msgId -> msgId
            | None -> MessageId.Earliest

    let redeliverMessages messages =
        async {
            do! this.Mb.PostAndAsyncReply(fun channel -> RedeliverUnacknowledged (messages, channel))
        } |> Async.StartImmediate

    let unAckedMessageTracker =
        if consumerConfig.AckTimeout > TimeSpan.Zero then
            if consumerConfig.AckTimeoutTickTime > TimeSpan.Zero then
                let tickDuration = if consumerConfig.AckTimeout > consumerConfig.AckTimeoutTickTime then consumerConfig.AckTimeoutTickTime else consumerConfig.AckTimeout
                UnAckedMessageTracker(prefix, consumerConfig.AckTimeout, tickDuration, redeliverMessages) :> IUnAckedMessageTracker
            else
                UnAckedMessageTracker(prefix, consumerConfig.AckTimeout, consumerConfig.AckTimeout, redeliverMessages) :> IUnAckedMessageTracker
        else
            UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED

    let negativeAcksTracker = NegativeAcksTracker(prefix, consumerConfig.NegativeAckRedeliveryDelay, redeliverMessages)

    let getConnectionState() = connectionHandler.ConnectionState
    let sendAckPayload (cnx: ClientCnx) payload = cnx.Send payload

    let acksGroupingTracker =
        if consumerConfig.Topic.IsPersistent then
            AcknowledgmentsGroupingTracker(prefix, consumerId, consumerConfig.AcknowledgementsGroupTime, getConnectionState, sendAckPayload) :> IAcknowledgmentsGroupingTracker
        else
            AcknowledgmentsGroupingTracker.NonPersistentAcknowledgmentGroupingTracker

    let sendAcknowledge (messageId: MessageId) ackType =
        async {
            match ackType with
            | AckType.Individual ->
                match messageId.Type with
                | Individual ->
                    unAckedMessageTracker.Remove(messageId) |> ignore
                | Cumulative batch ->
                    unAckedMessageTracker.Remove(messageId) |> ignore
            | AckType.Cumulative ->
                ()
            do! acksGroupingTracker.AddAcknowledgment(messageId, ackType)
            // Consumer acknowledgment operation immediately succeeds. In any case, if we're not able to send ack to broker,
            // the messages will be re-delivered
        }

    let markAckForBatchMessage (msgId: MessageId) ackType (batchDetails: BatchDetails) =
        let (batchIndex, batchAcker) = batchDetails
        let isAllMsgsAcked =
            match ackType with
            | AckType.Individual ->
                batchAcker.AckIndividual(batchIndex)
            | AckType.Cumulative ->
                batchAcker.AckGroup(batchIndex)
        let outstandingAcks = batchAcker.GetOutstandingAcks()
        let batchSize = batchAcker.GetBatchSize()
        if isAllMsgsAcked then
            Log.Logger.LogDebug("{0} can ack message acktype {1}, cardinality {2}, length {3}",
                prefix, ackType, outstandingAcks, batchSize)
            true
        else
            match ackType with
            | AckType.Cumulative when not batchAcker.PrevBatchCumulativelyAcked ->
                sendAcknowledge msgId.PrevBatchMessageId ackType |> Async.StartImmediate
                Log.Logger.LogDebug("{0} update PrevBatchCumulativelyAcked", prefix)
                batchAcker.PrevBatchCumulativelyAcked <- true
            | _ ->
                ()
            Log.Logger.LogDebug("{0} cannot ack message acktype {1}, cardinality {2}, length {3}",
                prefix, ackType, outstandingAcks, batchSize)
            false

    let isPriorEntryIndex idx =
        match startMessageId with
        | None -> false
        | Some startMsgId ->
            if consumerConfig.ResetIncludeHead then
                idx < startMsgId.EntryId
            else
                idx <= startMsgId.EntryId

    let isPriorBatchIndex idx =
        match startMessageId with
        | None -> false
        | Some startMsgId ->
            match startMsgId.Type with
            | Individual -> false
            | Cumulative (batchIndex, _) ->
                if consumerConfig.ResetIncludeHead then
                    idx < batchIndex
                else
                    idx <= batchIndex

    let isNonDurableAndSameEntryAndLedger (msgId: MessageId) =
        match startMessageId with
        | None ->
            false
        | Some startMsgId ->
            subscriptionMode = SubscriptionMode.NonDurable && startMsgId.LedgerId = msgId.LedgerId && startMsgId.EntryId = msgId.EntryId

    let receiveIndividualMessagesFromBatch (rawMessage: Message) =
        let batchSize = rawMessage.Metadata.NumMessages
        let acker = BatchMessageAcker(batchSize)
        let mutable skippedMessages = 0
        use stream = new MemoryStream(rawMessage.Payload)
        use binaryReader = new BinaryReader(stream)
        for i in 0..batchSize-1 do
            Log.Logger.LogDebug("{0} processing message num - {1} in batch", prefix, i)
            let singleMessageMetadata = Serializer.DeserializeWithLengthPrefix<SingleMessageMetadata>(stream, PrefixStyle.Fixed32BigEndian)
            let singleMessagePayload = binaryReader.ReadBytes(singleMessageMetadata.PayloadSize)

            if isNonDurableAndSameEntryAndLedger(rawMessage.MessageId) && isPriorBatchIndex(%i) then
                Log.Logger.LogDebug("{0} Ignoring message from before the startMessageId: {1} in batch", prefix, startMessageId)
                skippedMessages <- skippedMessages + 1
            else
                let messageId =
                    {
                        LedgerId = rawMessage.MessageId.LedgerId
                        EntryId = rawMessage.MessageId.EntryId
                        Partition = partitionIndex
                        Type = Cumulative(%i, acker)
                        TopicName = %""
                    }
                let message =
                    { rawMessage with
                        MessageId = messageId
                        Payload = singleMessagePayload
                        Properties =
                            if singleMessageMetadata.Properties.Count > 0 then
                                singleMessageMetadata.Properties
                                |> Seq.map (fun prop -> (prop.Key, prop.Value))
                                |> dict
                            else
                                EmptyProps
                        MessageKey = %singleMessageMetadata.PartitionKey
                    }
                incomingMessages.Enqueue(message)
        if skippedMessages > 0 then
            increaseAvailablePermits skippedMessages


    /// Record the event that one message has been processed by the application.
    /// Periodically, it sends a Flow command to notify the broker that it can push more messages
    let messageProcessed (msg: Message) =
        lastDequeuedMessage <- msg.MessageId
        increaseAvailablePermits 1
        if consumerConfig.AckTimeout <> TimeSpan.Zero then
            unAckedMessageTracker.Add msg.MessageId |> ignore

    let removeExpiredMessagesFromQueue (msgIds: RedeliverSet) =
        if incomingMessages.Count > 0 then
            let peek = incomingMessages.Peek()
            if msgIds.Contains peek.MessageId then
                // try not to remove elements that are added while we remove
                let mutable finish = false
                let mutable messagesFromQueue = 0
                while not finish && incomingMessages.Count > 0 do
                    let message = incomingMessages.Dequeue()
                    messagesFromQueue <- messagesFromQueue + 1
                    if msgIds.Contains(message.MessageId) |> not then
                        msgIds.Add(message.MessageId) |> ignore
                        finish <- true
                messagesFromQueue
            else
                // first message is not expired, then no message is expired in queue.
                0
        else
            0

    let replyWithMessage (channel: AsyncReplyChannel<MessageOrException>) message =
        messageProcessed message
        channel.Reply (Message message)

    let stopConsumer (state: ConsumerState) =
        unAckedMessageTracker.Close()
        negativeAcksTracker.Close()
        cleanup(this)
        if state.WaitingChannel <> nullChannel then
            state.WaitingChannel.Reply(Exn (AlreadyClosedException("Consumer is already closed")))
        Log.Logger.LogInformation("{0} stopped", prefix)

    let decompress message =
        let compressionCodec = message.Metadata.CompressionType |> CompressionCodec.create
        let uncompressedPayload = compressionCodec.Decode message.Metadata.UncompressedMessageSize message.Payload
        { message with Payload = uncompressedPayload }

    let mb = MailboxProcessor<ConsumerMessage>.Start(fun inbox ->

        let rec loop (state: ConsumerState) =
            async {
                let! msg = inbox.Receive()
                match msg with
                | ConsumerMessage.ConnectionOpened ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        Log.Logger.LogInformation("{0} starting subscribe to topic {1}", prefix, consumerConfig.Topic)
                        clientCnx.AddConsumer consumerId this.Mb
                        let requestId = Generators.getNextRequestId()
                        let isDurable = subscriptionMode = SubscriptionMode.Durable
                        startMessageId <-
                            if isDurable then
                                None
                            else
                                getStartMessageId() |> Some
                        let payload =
                            Commands.newSubscribe
                                consumerConfig.Topic.CompleteTopicName consumerConfig.SubscriptionName
                                consumerId requestId consumerConfig.ConsumerName consumerConfig.SubscriptionType
                                consumerConfig.SubscriptionInitialPosition consumerConfig.ReadCompacted startMessageId isDurable
                        try
                            let! response = clientCnx.SendAndWaitForReply requestId payload |> Async.AwaitTask
                            response |> PulsarResponseType.GetEmpty
                            Log.Logger.LogInformation("{0} subscribed", prefix)
                            connectionHandler.ResetBackoff()
                            let initialFlowCount = consumerConfig.ReceiverQueueSize
                            subscribeTsc.TrySetResult() |> ignore
                            if initialFlowCount <> 0 then
                                let flowCommand = Commands.newFlow consumerId initialFlowCount
                                let! success = clientCnx.Send flowCommand
                                if success then
                                    Log.Logger.LogDebug("{0} initial flow sent {1}", prefix, initialFlowCount)
                                else
                                    raise (ConnectionFailedOnSend "FlowCommand")
                        with
                        | ex ->
                            clientCnx.RemoveConsumer consumerId
                            Log.Logger.LogError(ex, "{0} failed to subscribe to topic", prefix)
                            if (connectionHandler.IsRetriableError ex && DateTime.Now < subscribeTimeout) then
                                connectionHandler.ReconnectLater ex
                            else
                                if not subscribeTsc.Task.IsCompleted then
                                    // unable to create new consumer, fail operation
                                    connectionHandler.Failed()
                                    subscribeTsc.SetException(ex)
                                    stopConsumer state
                                else
                                    connectionHandler.ReconnectLater ex
                    | _ ->
                        Log.Logger.LogWarning("{0} connection opened but connection is not ready", prefix)
                    return! loop state

                | ConsumerMessage.ConnectionClosed clientCnx ->

                    Log.Logger.LogDebug("{0} ConnectionClosed", prefix)
                    let clientCnx = clientCnx :?> ClientCnx
                    connectionHandler.ConnectionClosed clientCnx
                    clientCnx.RemoveConsumer(consumerId)
                    return! loop state

                | ConsumerMessage.ConnectionFailed ex ->

                    Log.Logger.LogDebug("{0} ConnectionFailed", prefix)
                    if (DateTime.Now > subscribeTimeout && subscribeTsc.TrySetException(ex)) then
                        Log.Logger.LogInformation("{0} creation failed", prefix)
                        connectionHandler.Failed()
                        stopConsumer state
                    else
                        return! loop state

                | ConsumerMessage.MessageReceived rawMessage ->

                    let hasWaitingChannel = state.WaitingChannel <> nullChannel
                    let msgId = { rawMessage.MessageId with Partition = partitionIndex }
                    Log.Logger.LogDebug("{0} MessageReceived {1} queueLength={2}, hasWaitingChannel={3}",
                        prefix, msgId, incomingMessages.Count, hasWaitingChannel)

                    if (acksGroupingTracker.IsDuplicate(msgId)) then
                        Log.Logger.LogInformation("{0} Ignoring message as it was already being acked earlier by same consumer {1}", prefix, msgId)
                        increaseAvailablePermits rawMessage.Metadata.NumMessages
                    else
                        if (rawMessage.Metadata.NumMessages = 1 && not rawMessage.Metadata.HasNumMessagesInBatch) then
                            if isNonDurableAndSameEntryAndLedger(rawMessage.MessageId) && isPriorEntryIndex(rawMessage.MessageId.EntryId) then
                                // We need to discard entries that were prior to startMessageId
                                Log.Logger.LogDebug("{0} Ignoring message from before the startMessageId: {1}", prefix, startMessageId)
                                return! loop state
                            else
                                let message = { rawMessage with MessageId = msgId } |> decompress
                                if not hasWaitingChannel then
                                    incomingMessages.Enqueue(message)
                                    return! loop state
                                else
                                    if (incomingMessages.Count = 0) then
                                        replyWithMessage state.WaitingChannel message
                                    else
                                        incomingMessages.Enqueue(message)
                                        replyWithMessage state.WaitingChannel <| incomingMessages.Dequeue()
                                    return! loop { state with WaitingChannel = nullChannel }
                        elif rawMessage.Metadata.NumMessages > 0 then
                            // handle batch message enqueuing; uncompressed payload has all messages in batch
                            receiveIndividualMessagesFromBatch (rawMessage |> decompress)
                            if hasWaitingChannel && incomingMessages.Count > 0 then
                                replyWithMessage state.WaitingChannel <| incomingMessages.Dequeue()
                                return! loop { state with WaitingChannel = nullChannel }
                            else
                                return! loop state
                        else
                            Log.Logger.LogWarning("{0} Received message with nonpositive numMessages: {1}", prefix, rawMessage.Metadata.NumMessages)
                            return! loop state

                | ConsumerMessage.Receive ch ->

                    Log.Logger.LogDebug("{0} Receive", prefix)
                    if incomingMessages.Count > 0 then
                        replyWithMessage ch <| incomingMessages.Dequeue()
                        return! loop state
                    else
                        Log.Logger.LogDebug("{0} GetMessage waiting", prefix)
                        return! loop { state with WaitingChannel = ch }

                | ConsumerMessage.Acknowledge (messageId, ackType, channel) ->

                    Log.Logger.LogDebug("{0} Acknowledge", prefix)
                    match connectionHandler.ConnectionState with
                    | Ready _ ->
                        match messageId.Type with
                        | Cumulative batchDetails when not (markAckForBatchMessage messageId ackType batchDetails) ->
                            // other messages in batch are still pending ack.
                            ()
                        | _ ->
                            do! sendAcknowledge messageId ackType
                            Log.Logger.LogDebug("{0} acknowledged message - {1}, acktype {2}", prefix, messageId, ackType)
                        channel.Reply(true)
                    | _ ->
                        Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                        channel.Reply(false)
                    return! loop state

                | ConsumerMessage.RedeliverUnacknowledged (messageIds, channel) ->

                    Log.Logger.LogDebug("{0} RedeliverUnacknowledged", prefix)
                    match consumerConfig.SubscriptionType with
                    | SubscriptionType.Shared | SubscriptionType.KeyShared ->
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx ->
                            let messagesFromQueue = removeExpiredMessagesFromQueue(messageIds);
                            let batches = messageIds |> Seq.chunkBySize MAX_REDELIVER_UNACKNOWLEDGED
                            for batch in batches do
                                let command = Commands.newRedeliverUnacknowledgedMessages consumerId (Some(batch))
                                let! success = clientCnx.Send command
                                if success then
                                    Log.Logger.LogDebug("{0} RedeliverAcknowledged complete", prefix)
                                else
                                    Log.Logger.LogWarning("{0} RedeliverAcknowledged was not complete", prefix)
                            if messagesFromQueue > 0 then
                                increaseAvailablePermits messagesFromQueue
                        | _ ->
                            Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                        channel.Reply()
                    | _ ->
                        this.Mb.Post(RedeliverAllUnacknowledged channel)
                        Log.Logger.LogInformation("{0} We cannot redeliver single messages if subscription type is not Shared", prefix)
                    return! loop state

                | ConsumerMessage.RedeliverAllUnacknowledged channel ->

                    Log.Logger.LogDebug("{0} RedeliverAllUnacknowledged", prefix)
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        let command = Commands.newRedeliverUnacknowledgedMessages consumerId None
                        let! success = clientCnx.Send command
                        if success then
                            let currentSize = incomingMessages.Count
                            if currentSize > 0 then
                                incomingMessages.Clear()
                                increaseAvailablePermits currentSize
                                unAckedMessageTracker.Clear()
                            Log.Logger.LogDebug("{0} RedeliverAllUnacknowledged complete", prefix)
                        else
                            Log.Logger.LogWarning("{0} RedeliverAllUnacknowledged was not complete", prefix)
                    | _ ->
                        Log.Logger.LogWarning("{0} not connected, skipping send", prefix)
                    channel.Reply()
                    return! loop state

                | ConsumerMessage.SeekAsync (seekData, channel) ->

                    Log.Logger.LogDebug("{0} SeekAsync", prefix)
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        let requestId = Generators.getNextRequestId()
                        Log.Logger.LogInformation("{0} Seek subscription to {1}", prefix, seekData);
                        task {
                            try
                                let (payload, lastMessage) =
                                    match seekData with
                                    | Timestamp timestamp -> Commands.newSeekByTimestamp consumerId requestId timestamp, MessageId.Earliest
                                    | MessageId messageId -> Commands.newSeekByMsgId consumerId requestId messageId, messageId
                                let! response = clientCnx.SendAndWaitForReply requestId payload
                                response |> PulsarResponseType.GetEmpty
                                lastDequeuedMessage <- lastMessage
                                acksGroupingTracker.FlushAndClean()
                                incomingMessages.Clear()
                                Log.Logger.LogInformation("{0} Successfully reset subscription to {1}", prefix, seekData)
                            with
                            | ex ->
                                Log.Logger.LogError(ex, "{0} Failed to reset subscription to {1}", prefix, seekData)
                                reraize ex
                        } |> channel.Reply
                        return! loop state
                    | _ ->
                        channel.Reply(Task.FromException(NotConnectedException "Not connected to broker"))
                        Log.Logger.LogError("{0} not connected, skipping SeekAsync {1}", prefix, seekData)
                        return! loop state

                | ConsumerMessage.SendFlowPermits numMessages ->

                    Log.Logger.LogDebug("{0} SendFlowPermits {1}", prefix, numMessages)
                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        let flowCommand = Commands.newFlow consumerId numMessages
                        let! success = clientCnx.Send flowCommand
                        if not success then
                            Log.Logger.LogWarning("{0} failed SendFlowPermits {1}", prefix, numMessages)
                    | _ ->
                        Log.Logger.LogWarning("{0} not connected, skipping SendFlowPermits {1}", prefix, numMessages)
                    return! loop state

                | ConsumerMessage.ReachedEndOfTheTopic ->

                    Log.Logger.LogWarning("{0} ReachedEndOfTheTopic", prefix)
                    hasReachedEndOfTopic <- true

                | ConsumerMessage.HasMessageAvailable channel ->

                    Log.Logger.LogDebug("{0} HasMessageAvailable", prefix)
                    if hasMoreMessages lastMessageIdInBroker lastDequeuedMessage then
                        channel.Reply(Task.FromResult(true))
                    else
                        match connectionHandler.ConnectionState with
                        | Ready clientCnx ->
                            let requestId = Generators.getNextRequestId()
                            let payload = Commands.newGetLastMessageId consumerId requestId
                            task {
                                try
                                    let! response = clientCnx.SendAndWaitForReply requestId payload
                                    let lastMessageId = response |> PulsarResponseType.GetLastMessageId
                                    lastMessageIdInBroker <- lastMessageId
                                    return hasMoreMessages lastMessageIdInBroker lastDequeuedMessage
                                with
                                | ex ->
                                    Log.Logger.LogError(ex, "{0} failed getLastMessageId", prefix)
                                    reraize ex
                                    return false // not executed, needed for typecheck
                            } |> channel.Reply
                        | _ ->
                            channel.Reply(Task.FromException(NotConnectedException "Not connected to broker") :?> Task<bool>)
                            Log.Logger.LogError("{0} not connected, skipping HasMessageAvailable {1}", prefix)
                    return! loop state

                | ConsumerMessage.Close channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        Log.Logger.LogInformation("{0} starting close", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newCloseConsumer consumerId requestId
                        task {
                            try
                                let! response = clientCnx.SendAndWaitForReply requestId payload
                                response |> PulsarResponseType.GetEmpty
                                clientCnx.RemoveConsumer(consumerId)
                                connectionHandler.Closed()
                                stopConsumer state
                            with
                            | ex ->
                                Log.Logger.LogError(ex, "{0} failed to close", prefix)
                                reraize ex
                        } |> channel.Reply
                    | _ ->
                        Log.Logger.LogInformation("{0} closing but current state {1}", prefix, connectionHandler.ConnectionState)
                        connectionHandler.Closed()
                        stopConsumer state
                        channel.Reply(Task.FromResult())

                | ConsumerMessage.Unsubscribe channel ->

                    match connectionHandler.ConnectionState with
                    | Ready clientCnx ->
                        connectionHandler.Closing()
                        unAckedMessageTracker.Close()
                        Log.Logger.LogInformation("{0} starting unsubscribe ", prefix)
                        let requestId = Generators.getNextRequestId()
                        let payload = Commands.newUnsubscribeConsumer consumerId requestId
                        task {
                            try
                                let! response = clientCnx.SendAndWaitForReply requestId payload
                                response |> PulsarResponseType.GetEmpty
                                clientCnx.RemoveConsumer(consumerId)
                                connectionHandler.Closed()
                                stopConsumer state
                            with
                            | ex ->
                                Log.Logger.LogError(ex, "{0} failed to unsubscribe", prefix)
                                reraize ex
                        } |> channel.Reply
                    | _ ->
                        Log.Logger.LogError("{0} can't unsubscribe since not connected", prefix)
                        channel.Reply(Task.FromException(Exception("Not connected to broker")))
                        return! loop state

            }
        loop { WaitingChannel = nullChannel }
    )
    do mb.Error.Add(fun ex -> Log.Logger.LogCritical(ex, "{0} mailbox failure", prefix))

    member private this.Mb with get(): MailboxProcessor<ConsumerMessage> = mb

    member this.ConsumerId with get() = consumerId

    member this.HasMessageAvailableAsync() =
        task {
            connectionHandler.CheckIfActive()
            let! result = mb.PostAndAsyncReply(fun channel -> HasMessageAvailable channel)
            return! result
        }

    override this.Equals consumer =
        consumerId = (consumer :?> ConsumerImpl).ConsumerId

    override this.GetHashCode () = int consumerId

    member internal this.InitInternal() =
        task {
            do connectionHandler.GrabCnx()
            return! subscribeTsc.Task
        }

    static member Init(consumerConfig: ConsumerConfiguration, clientConfig: PulsarClientConfiguration, connectionPool: ConnectionPool,
                       partitionIndex: int, subscriptionMode: SubscriptionMode, startMessageId: MessageId option, lookup: BinaryLookupService,
                       cleanup: ConsumerImpl -> unit) =
        task {
            let consumer = ConsumerImpl(consumerConfig, clientConfig, connectionPool, partitionIndex, subscriptionMode, startMessageId, lookup, cleanup)
            do! consumer.InitInternal()
            return consumer :> IConsumer
        }

    interface IConsumer with

        member this.ReceiveAsync() =
            task {
                connectionHandler.CheckIfActive()
                match! mb.PostAndAsyncReply(Receive) with
                | Message msg ->
                    return msg
                | Exn exn ->
                    reraize exn
                    return Unchecked.defaultof<Message>
            }

        member this.AcknowledgeAsync (msgId: MessageId) =
            task {
                connectionHandler.CheckIfActive()
                let! success = mb.PostAndAsyncReply(fun channel -> Acknowledge (msgId, AckType.Individual, channel))
                if not success then
                    raise (ConnectionFailedOnSend "AcknowledgeAsync")
            }

        member this.AcknowledgeCumulativeAsync (msgId: MessageId) =
            task {
                connectionHandler.CheckIfActive()
                let! success = mb.PostAndAsyncReply(fun channel -> Acknowledge (msgId, AckType.Cumulative, channel))
                if not success then
                    raise (ConnectionFailedOnSend "AcknowledgeCumulativeAsync")
            }

        member this.RedeliverUnacknowledgedMessagesAsync () =
            task {
                connectionHandler.CheckIfActive()
                do! mb.PostAndAsyncReply(RedeliverAllUnacknowledged)
            }

        member this.SeekAsync (messageId: MessageId) =
            task {
                connectionHandler.CheckIfActive()
                let! result = mb.PostAndAsyncReply(fun channel -> SeekAsync (MessageId messageId, channel))
                return! result
            }

        member this.SeekAsync (timestamp: uint64) =
            task {
                connectionHandler.CheckIfActive()
                let! result = mb.PostAndAsyncReply(fun channel -> SeekAsync (Timestamp timestamp, channel))
                return! result
            }

        member this.CloseAsync() =
            task {
                match connectionHandler.ConnectionState with
                | Closing | Closed ->
                    return ()
                | _ ->
                    let! result = mb.PostAndAsyncReply(ConsumerMessage.Close)
                    return! result
            }

        member this.UnsubscribeAsync() =
            task {
                connectionHandler.CheckIfActive()
                let! result = mb.PostAndAsyncReply(ConsumerMessage.Unsubscribe)
                return! result
            }

        member this.HasReachedEndOfTopic with get() = hasReachedEndOfTopic

        member this.NegativeAcknowledge msgId =
            task {
                negativeAcksTracker.Add(msgId) |> ignore
                // Ensure the message is not redelivered for ack-timeout, since we did receive an "ack"
                unAckedMessageTracker.Remove(msgId) |> ignore
            }

        member this.Topic with get() = %consumerConfig.Topic.CompleteTopicName




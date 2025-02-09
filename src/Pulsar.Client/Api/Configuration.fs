﻿namespace Pulsar.Client.Api

open Pulsar.Client.Common
open System
open System.Security.Cryptography.X509Certificates

type PulsarClientConfiguration =
    {
        ServiceUrl: Uri
        OperationTimeout: TimeSpan
        MaxNumberOfRejectedRequestPerConnection: int
        UseTls: bool
        TlsHostnameVerificationEnable: bool
        TlsAllowInsecureConnection: bool
        TlsTrustCertificate: X509Certificate2
        Authentication: Authentication
    }
    static member Default =
        {
            ServiceUrl = null
            OperationTimeout = TimeSpan.FromMilliseconds(30000.0)
            MaxNumberOfRejectedRequestPerConnection = 50
            UseTls = false
            TlsHostnameVerificationEnable = false
            TlsAllowInsecureConnection = false
            TlsTrustCertificate = null
            Authentication = Authentication.AuthenticationDisabled
        }

type ConsumerConfiguration =
    {
        Topic: TopicName
        ConsumerName: string
        SubscriptionName: string
        SubscriptionType: SubscriptionType
        ReceiverQueueSize: int
        MaxTotalReceiverQueueSizeAcrossPartitions: int
        SubscriptionInitialPosition: SubscriptionInitialPosition
        AckTimeout: TimeSpan
        AckTimeoutTickTime: TimeSpan
        AcknowledgementsGroupTime: TimeSpan
        AutoUpdatePartitions: bool
        ReadCompacted: bool
        NegativeAckRedeliveryDelay: TimeSpan
        ResetIncludeHead: bool
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ConsumerName = ""
            SubscriptionName = ""
            SubscriptionType = SubscriptionType.Exclusive
            ReceiverQueueSize = 1000
            MaxTotalReceiverQueueSizeAcrossPartitions = 50000
            SubscriptionInitialPosition = SubscriptionInitialPosition.Latest
            AckTimeout = TimeSpan.Zero
            AckTimeoutTickTime = TimeSpan.FromMilliseconds(1000.0)
            AcknowledgementsGroupTime = TimeSpan.FromMilliseconds(100.0)
            AutoUpdatePartitions = true
            ReadCompacted = false
            NegativeAckRedeliveryDelay = TimeSpan.FromMinutes(1.0)
            ResetIncludeHead = false
        }

type ProducerConfiguration =
    {
        Topic: TopicName
        ProducerName: string
        MaxPendingMessagesAcrossPartitions: int
        MaxPendingMessages: int
        BatchingEnabled: bool
        MaxMessagesPerBatch: int
        MaxBatchingPublishDelay: TimeSpan
        SendTimeout: TimeSpan
        CompressionType: CompressionType
        MessageRoutingMode: MessageRoutingMode
        CustomMessageRouter: IMessageRouter
        AutoUpdatePartitions: bool
        HashingScheme: HashingScheme
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            ProducerName = ""
            MaxPendingMessages = 1000
            MaxPendingMessagesAcrossPartitions = 50000
            BatchingEnabled = true
            MaxMessagesPerBatch = 1000
            MaxBatchingPublishDelay = TimeSpan.FromMilliseconds(1.0)
            SendTimeout = TimeSpan.FromMilliseconds(30000.0)
            CompressionType = CompressionType.None
            MessageRoutingMode = MessageRoutingMode.RoundRobinPartition
            CustomMessageRouter = Unchecked.defaultof<IMessageRouter>
            AutoUpdatePartitions = true
            HashingScheme = HashingScheme.DotnetStringHash
        }

type ReaderConfiguration =
    {
        Topic: TopicName
        StartMessageId: MessageId
        SubscriptionRolePrefix: string
        ReceiverQueueSize: int
        ReadCompacted: bool
        ReaderName: string
        ResetIncludeHead: bool
    }
    static member Default =
        {
            Topic = Unchecked.defaultof<TopicName>
            StartMessageId = Unchecked.defaultof<MessageId>
            SubscriptionRolePrefix = ""
            ReceiverQueueSize = 1000
            ReadCompacted = false
            ReaderName = ""
            ResetIncludeHead = false
        }
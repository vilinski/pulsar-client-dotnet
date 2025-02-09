﻿module Pulsar.Client.IntegrationTests.Keys

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open System.Threading
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests.Common
open FSharp.UMX
open FSharp.Control

[<Tests>]
let tests =

    testList "keys" [

        testAsync "Keys and properties are propertly passed" {

            Log.Debug("Started Keys and properties are propertly passed")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "propsTestProducer"
            let consumerName = "propsTestConsumer"

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessagesWithProps producer 100 producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessagesWithProps consumer 100 consumerName
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Keys and properties are propertly passed")
        }

        testAsync "Messages with same key always go to the same consumer" {

            Log.Debug("Started Messages with same key always go to the same consumer")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let consumerName1 = "PartitionedConsumer1"
            let consumerName2 = "PartitionedConsumer2"
            let producerName = "PartitionedProducer"

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer1 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.KeyShared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName1)
                    .SubscribeAsync() |> Async.AwaitTask

            let! consumer2 =
                ConsumerBuilder(client)
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscriptionType(SubscriptionType.KeyShared)
                    .AcknowledgementsGroupTime(TimeSpan.FromMilliseconds(50.0))
                    .ConsumerName(consumerName2)
                    .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        let firstKey = "111111"
                        let secondKey = "444444"
                        let getMessageBuilder key i =
                            MessageBuilder(Encoding.UTF8.GetBytes(key + "Hello" + i), key)
                        let! _ = producer.SendAsync(getMessageBuilder firstKey "0")
                        let! _ = producer.SendAsync(getMessageBuilder secondKey "0")
                        let! _ = producer.SendAsync(getMessageBuilder firstKey "1")
                        let! _ = producer.SendAsync(getMessageBuilder secondKey "1")
                        let! _ = producer.SendAsync(getMessageBuilder secondKey "2")
                        let! _ = producer.SendAsync(getMessageBuilder firstKey "2")
                        ()
                    }:> Task)

            let consumer1Task =
                Task.Run(fun () ->
                    task {
                        let! msg1 = consumer1.ReceiveAsync()
                        let! msg2 = consumer1.ReceiveAsync()
                        let! msg3 = consumer1.ReceiveAsync()
                        let prefix = (string msg1.MessageKey).Substring(0,6)
                        [msg1;msg2;msg3]
                            |> List.iteri
                            (fun i elem ->
                                let strKey = string elem.MessageKey
                                let message = Encoding.UTF8.GetString(elem.Payload)
                                if (strKey = prefix && message.StartsWith(prefix) && message.EndsWith(i.ToString())) |> not then
                                    failwith <| sprintf "Incorrect key %s prefix %s consumer %s" strKey prefix consumerName1
                                else
                                    ()
                            )
                        Log.Debug("consumer1Task finished")
                    }:> Task)

            let consumer2Task =
                Task.Run(fun () ->
                    task {
                        let! msg1 = consumer2.ReceiveAsync()
                        let! msg2 = consumer2.ReceiveAsync()
                        let! msg3 = consumer2.ReceiveAsync()
                        let prefix = (string msg1.MessageKey).Substring(0,6)
                        [msg1;msg2;msg3]
                            |> List.iteri
                            (fun i elem ->
                                let strKey = string elem.MessageKey
                                let message = Encoding.UTF8.GetString(elem.Payload)
                                if (strKey = prefix && message.StartsWith(prefix) && message.EndsWith(i.ToString())) |> not then
                                    failwith <| sprintf "Incorrect key %s prefix %s consumer %s" strKey prefix consumerName2
                                else
                                    ()
                            )
                        Log.Debug("consumer2Task finished")
                    }:> Task)

            do! Task.WhenAll(producerTask, consumer1Task, consumer2Task) |> Async.AwaitTask

            Log.Debug("Finished Messages with same key always go to the same consumer")
        }

        // Should be run manually, should finish after https://github.com/apache/pulsar/issues/5384 is completed
        ptestAsync "Compacting works as expected" {

            Log.Debug("Started Keys and properties are propertly passed")
            let client = getClient()
            let topicName = "public/retention/topic-2" // + Guid.NewGuid().ToString("N")
            let producerName = "propsTestProducer"
            let consumerName = "propsTestConsumer"

            let! producer =
                ProducerBuilder(client)
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            //let! consumer =
            //    ConsumerBuilder(client)
            //        .Topic(topicName)
            //        .ConsumerName(consumerName)
            //        .SubscriptionName("test-subscription")
            //        .SubscribeAsync() |> Async.AwaitTask

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessagesWithSameKey producer 100 "test" producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        //do! consumeMessagesWithProps consumer 100 consumerName
                        return ()
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) |> Async.AwaitTask

            Log.Debug("Finished Keys and properties are propertly passed")
        }
    ]

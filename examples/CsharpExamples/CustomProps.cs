﻿using System;
using System.Text;
using System.Threading.Tasks;
using Pulsar.Client.Api;
using Pulsar.Client.Common;
using System.Collections.Generic;

namespace CsharpExamples
{
    internal class CustomProps
    {
        internal static async Task RunCustomProps()
        {
            const string serviceUrl = "pulsar://my-pulsar-cluster:31002";
            const string subscriptionName = "my-subscription";
            var topicName = $"my-topic-{DateTime.Now.Ticks}";

            var client = new PulsarClientBuilder()
                .ServiceUrl(serviceUrl)
                .Build();

            var producer = await new ProducerBuilder(client)
                .Topic(topicName)
                .CreateAsync();

            var consumer = await new ConsumerBuilder(client)
                .Topic(topicName)
                .SubscriptionName(subscriptionName)
                .SubscribeAsync();

            var payload = Encoding.UTF8.GetBytes($"Sent from C# at '{DateTime.Now}'");
            var messageId = await producer.SendAsync(new MessageBuilder(payload, "C#", new Dictionary<string, string> { ["1"] = "one" }));
            Console.WriteLine($"MessageId is: '{messageId}'");

            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Payload)} key: {message.MessageKey} prop1: {message.Properties["1"]}");

            await consumer.AcknowledgeAsync(message.MessageId);
        }
    }
}
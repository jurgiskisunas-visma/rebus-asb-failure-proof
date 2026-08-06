namespace RebusAsbFailureProof.Tests;

using Azure.Messaging.ServiceBus;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Retry.Simple;

/// <summary>Scratch tests used to work out what the emulator and Rebus actually do. Not part of the proof.</summary>
[TestClass]
[Ignore("Scratch diagnostics used while isolating the defect - kept as evidence, not part of the proof.")]
public class DiagnosticsTests
{
    [TestMethod]
    [Timeout(120_000)]
    public async Task Diag_DoTwoSendsWithTheSameMessageIdBothLandInTheQueue()
    {
        await DrainAsync(Emulator.DuplicateIdQueue);

        using var activator = new BuiltinHandlerActivator();
        var bus = Configure.With(activator)
            .Transport(t => t
                .UseAzureServiceBus(Emulator.ConnectionString, Emulator.DuplicateIdQueue)
                .AutomaticallyRenewPeekLock()
                .DoNotCreateQueues())
            .Options(o =>
            {
                o.SetNumberOfWorkers(0);
                o.SetMaxParallelism(1);
            })
            .Start();

        var sharedId = $"duplicate-{Guid.NewGuid()}";
        var headers = new Dictionary<string, string> { [Headers.MessageId] = sharedId };

        await bus.SendLocal(new ReproMessage("first"), headers);
        await bus.SendLocal(new ReproMessage("second"), headers);
        await Task.Delay(TimeSpan.FromSeconds(3));
        bus.Dispose();

        await using var client = new ServiceBusClient(Emulator.ConnectionString);
        var receiver = client.CreateReceiver(Emulator.DuplicateIdQueue);
        var peeked = await receiver.PeekMessagesAsync(10);

        Console.WriteLine($"intended MessageId: {sharedId}");
        Console.WriteLine($"messages in queue:  {peeked.Count}");
        foreach (var message in peeked)
        {
            Console.WriteLine($"  MessageId={message.MessageId} deliveryCount={message.DeliveryCount} body={message.Body}");
        }

        Assert.AreEqual(2, peeked.Count, "Expected both sends to land as separate messages.");
        Assert.IsTrue(peeked.All(m => m.MessageId == sharedId), "Expected Rebus to honour the supplied Headers.MessageId.");
    }

    [TestMethod]
    [Timeout(180_000)]
    public async Task Diag_WhatDoesRebusLogWhenTheSameMessageIdIsInFlightTwice()
    {
        await DrainAsync(Emulator.DuplicateIdQueue);

        var logs = new RecordingLoggerFactory();
        var dispatched = 0;
        using var activator = new BuiltinHandlerActivator();

        activator.Handle<ReproMessage>(async msg =>
        {
            var n = Interlocked.Increment(ref dispatched);
            Console.WriteLine($"  handler entered #{n} ({msg.Label}) at {DateTimeOffset.UtcNow:HH:mm:ss.fff}");
            await Task.Delay(TimeSpan.FromSeconds(8));
        });

        var bus = Configure.With(activator)
            .Logging(l => l.Use(logs))
            .Transport(t => t
                .UseAzureServiceBus(Emulator.ConnectionString, Emulator.DuplicateIdQueue)
                .AutomaticallyRenewPeekLock()
                .DoNotCreateQueues())
            .Options(o =>
            {
                o.SetNumberOfWorkers(4);
                o.SetMaxParallelism(4);
                o.RetryStrategy(errorQueueName: Emulator.ErrorQueue, maxDeliveryAttempts: 20);
            })
            .Start();

        var sharedId = $"duplicate-{Guid.NewGuid()}";
        var headers = new Dictionary<string, string> { [Headers.MessageId] = sharedId };
        await bus.SendLocal(new ReproMessage("first"), headers);
        await bus.SendLocal(new ReproMessage("second"), headers);

        await Task.Delay(TimeSpan.FromSeconds(25));
        bus.Dispose();

        Console.WriteLine($"dispatched: {dispatched}");
        Console.WriteLine("--- ALL log events ---");
        foreach (var e in logs.Events)
        {
            Console.WriteLine($"[{e.At:HH:mm:ss.fff}][{e.Level}] {e.Message}");
            if (e.Exception != null)
            {
                Console.WriteLine($"    EX {e.Exception.GetType().FullName}: {e.Exception.Message}");
            }
        }

        await using var client = new ServiceBusClient(Emulator.ConnectionString);
        var active = await client.CreateReceiver(Emulator.DuplicateIdQueue).PeekMessagesAsync(10);
        var dead = await client
            .CreateReceiver(Emulator.DuplicateIdQueue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter })
            .PeekMessagesAsync(10);

        Console.WriteLine($"QUEUESTATE active={active.Count} dead={dead.Count}");
        foreach (var m in active)
        {
            Console.WriteLine($"  ACTIVE id={m.MessageId} deliveryCount={m.DeliveryCount}");
        }

        foreach (var m in dead)
        {
            Console.WriteLine($"  DEAD id={m.MessageId} deliveryCount={m.DeliveryCount} reason={m.DeadLetterReason}");
        }
    }

    private static async Task DrainAsync(string queue)
    {
        await using var client = new ServiceBusClient(Emulator.ConnectionString);

        foreach (var options in new[]
                 {
                     new ServiceBusReceiverOptions(),
                     new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter },
                 })
        {
            var receiver = client.CreateReceiver(queue, options);
            while (true)
            {
                var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
                if (message == null)
                {
                    break;
                }

                await receiver.CompleteMessageAsync(message);
            }
        }
    }
}

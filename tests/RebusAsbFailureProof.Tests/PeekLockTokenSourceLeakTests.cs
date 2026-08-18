namespace RebusAsbFailureProof.Tests;

using Azure.Messaging.ServiceBus;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Retry.Simple;

/// <summary>
/// Regression tests for rebus-org/Rebus.AzureServiceBus#117, fixed in 10.7.1.
///
/// Before the fix, AzureServiceBusTransport.Receive tracked a per-message CancellationTokenSource
/// keyed by ServiceBus MessageId. MessageId identifies a *message*, but the tracked resource is a
/// *delivery* - and Azure Service Bus is at-least-once, so the same MessageId is legitimately in
/// flight twice whenever a peek lock lapses and the broker redelivers while the handler is still
/// running. TryAdd then failed, the token source was disposed, and .Token was read from the disposed
/// instance:
///
///     System.ObjectDisposedException: The CancellationTokenSource has been disposed.
///        at System.Threading.CancellationTokenSource.get_Token()
///        at Rebus.AzureServiceBus.AzureServiceBusTransport.Receive(...)
///        at Rebus.Workers.ThreadPoolBased.ThreadPoolWorker.ReceiveTransportMessage(...)
///
/// Because the throw happened inside Receive - before dispatch - no handler ran, the retry step
/// never ran, maxDeliveryAttempts never incremented, and the message could not reach the Rebus
/// error queue. It was redelivered every lock duration until MaxDeliveryCount was exhausted and the
/// broker dead-lettered it natively.
///
/// 10.7.1 keys the tracking by lock token, which is unique per delivery attempt. These tests assert
/// the fixed behaviour, so they PASS on 10.7.1+ and FAIL on 10.5.0 - 10.7.0.
/// </summary>
[TestClass]
public class PeekLockTokenSourceLeakTests
{
    /// <summary>
    /// The headline case: one message, one genuine broker redelivery after a lock lapse.
    ///
    /// repro.lockrecover has LockDuration PT5S and MaxDeliveryCount 5. The handler overruns the lock
    /// on its FIRST invocation only and returns immediately on every later one, so the redelivery is
    /// trivially processable - it needs milliseconds and has a full 5 seconds.
    ///
    /// Fixed behaviour: the redelivery is dispatched while delivery #1 is still in flight, the
    /// handler returns immediately, and the message is settled. Nothing is dead-lettered.
    /// </summary>
    [TestMethod]
    [Timeout(300_000)]
    public async Task RecoverableLockLapse_ThenTheRedeliveryIsDispatchedAndTheMessageIsSettled()
    {
        // Stage
        await DrainQueueAsync(Emulator.LockRecoverQueue);

        var logs = new RecordingLoggerFactory();
        var handlerInvocations = 0;
        var invocationTimes = new System.Collections.Concurrent.ConcurrentQueue<DateTimeOffset>();
        DateTimeOffset? firstDeliveryFinishedAt = null;
        using var activator = new BuiltinHandlerActivator();

        activator.Handle<ReproMessage>(async _ =>
        {
            var invocation = Interlocked.Increment(ref handlerInvocations);
            invocationTimes.Enqueue(DateTimeOffset.UtcNow);
            Console.WriteLine($"  handler invocation #{invocation} at {DateTimeOffset.UtcNow:HH:mm:ss.fff}");

            if (invocation == 1)
            {
                // Overrun the 5s lock on the first delivery only, and stay in flight long enough
                // for the broker to redeliver.
                await Task.Delay(TimeSpan.FromSeconds(40));
                firstDeliveryFinishedAt = DateTimeOffset.UtcNow;
            }
        });

        var bus = Configure.With(activator)
            .Logging(l => l.Use(logs))
            .Transport(t => t
                .UseAzureServiceBus(Emulator.ConnectionString, Emulator.LockRecoverQueue)
                .AutomaticallyRenewPeekLock()
                .DoNotCreateQueues()
                .DoNotCheckQueueConfiguration())
            .Options(o =>
            {
                o.SetNumberOfWorkers(3);
                o.SetMaxParallelism(3);

                // Deliberately high, so anything reaching the error queue is Rebus doing it and
                // anything dead-lettered is the broker doing it.
                o.RetryStrategy(errorQueueName: Emulator.ErrorQueue, maxDeliveryAttempts: 50);
            })
            .Start();

        // Act
        await bus.SendLocal(new ReproMessage("recoverable-lock-lapse"));
        await Task.Delay(TimeSpan.FromSeconds(70));
        bus.Dispose();

        var disposedFailures = logs.WithException<ObjectDisposedException>();
        var overlapEnd = firstDeliveryFinishedAt ?? DateTimeOffset.MaxValue;
        var dispatchesDuringOverlap = invocationTimes.Skip(1).Count(t => t < overlapEnd);
        var deadLettered = await PeekDeadLetterQueueAsync(Emulator.LockRecoverQueue);

        // Assert
        Console.WriteLine($"handler invocations:                    {handlerInvocations}");
        Console.WriteLine($"first delivery finished at:             {firstDeliveryFinishedAt:HH:mm:ss.fff}");
        Console.WriteLine($"ObjectDisposedException events:         {disposedFailures.Length}");
        Console.WriteLine($"redeliveries dispatched during overlap: {dispatchesDuringOverlap}");
        Console.WriteLine($"dead-lettered messages:                 {deadLettered.Count}");

        Assert.AreEqual(
            0,
            disposedFailures.Length,
            "Receive threw ObjectDisposedException. The per-delivery CancellationTokenSource is " +
            "being keyed by something that collides across concurrent deliveries of one message.");

        Assert.IsTrue(
            dispatchesDuringOverlap > 0,
            "The redelivery that arrived while delivery #1 was still in flight was never dispatched. " +
            "It should have been - the handler returns in milliseconds on any invocation after the " +
            "first, and had a full lock duration to do it.");

        Assert.AreEqual(
            0,
            deadLettered.Count,
            "The message was dead-lettered by the broker even though it was trivially processable.");

        Assert.AreEqual(
            0,
            (await PeekQueueAsync(Emulator.ErrorQueue)).Count,
            "Nothing should have reached the Rebus error queue either.");
    }

    /// <summary>
    /// Minimal isolation of the same collision, without waiting on lock timing: two sends sharing
    /// one explicitly-set MessageId, delivered concurrently. Supported usage - it is how ASB
    /// duplicate detection is driven, and Rebus honours the header.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public async Task Receive_WhenSameMessageIdIsInFlightTwice_ThenBothAreDispatchedWithoutError()
    {
        await AssertConcurrentSameMessageIdIsHandledCleanlyAsync(automaticallyRenewPeekLock: true);
    }

    /// <summary>
    /// Before the fix the token source was created outside the
    /// `AutomaticallyRenewPeekLock &amp;&amp; !_prefetchingEnabled` guard, so the defect also hit users who
    /// never enabled peek lock renewal. Same scenario with renewal off.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public async Task Receive_WhenSameMessageIdIsInFlightTwiceAndRenewalIsDisabled_ThenBothAreDispatchedWithoutError()
    {
        await AssertConcurrentSameMessageIdIsHandledCleanlyAsync(automaticallyRenewPeekLock: false);
    }

    private static async Task AssertConcurrentSameMessageIdIsHandledCleanlyAsync(bool automaticallyRenewPeekLock)
    {
        // Stage
        await DrainQueueAsync(Emulator.DuplicateIdQueue);

        var logs = new RecordingLoggerFactory();
        var dispatched = 0;
        using var activator = new BuiltinHandlerActivator();

        activator.Handle<ReproMessage>(async _ =>
        {
            Interlocked.Increment(ref dispatched);
            await Task.Delay(TimeSpan.FromSeconds(6));
        });

        var bus = StartDuplicateIdBus(activator, logs, automaticallyRenewPeekLock);

        // Act - two messages carrying the SAME ServiceBus MessageId, delivered concurrently.
        await SendTwiceWithOneMessageIdAsync(bus);
        await Task.Delay(TimeSpan.FromSeconds(20));
        bus.Dispose();

        // Assert
        var disposedFailures = logs.WithException<ObjectDisposedException>();
        Console.WriteLine($"renewal enabled:                   {automaticallyRenewPeekLock}");
        Console.WriteLine($"dispatched to handler:             {dispatched}");
        Console.WriteLine($"ObjectDisposedException log events: {disposedFailures.Length}");
        foreach (var failure in disposedFailures.Take(3))
        {
            Console.WriteLine($"  [{failure.Level}] {failure.Message}");
        }

        Assert.AreEqual(
            0,
            disposedFailures.Length,
            "Receive threw ObjectDisposedException for the second concurrent delivery of the same MessageId.");

        Assert.AreEqual(
            2,
            dispatched,
            "Both messages should have been dispatched to the handler.");
    }

    private static Rebus.Bus.IBus StartDuplicateIdBus(
        BuiltinHandlerActivator activator,
        RecordingLoggerFactory logs,
        bool automaticallyRenewPeekLock) =>
        Configure.With(activator)
            .Logging(l => l.Use(logs))
            .Transport(t =>
            {
                var settings = t.UseAzureServiceBus(Emulator.ConnectionString, Emulator.DuplicateIdQueue);

                if (automaticallyRenewPeekLock)
                {
                    settings = settings.AutomaticallyRenewPeekLock();
                }

                settings.DoNotCreateQueues().DoNotCheckQueueConfiguration();
            })
            .Options(o =>
            {
                o.SetNumberOfWorkers(3);
                o.SetMaxParallelism(3);
                o.RetryStrategy(errorQueueName: Emulator.ErrorQueue, maxDeliveryAttempts: 20);
            })
            .Start();

    private static async Task SendTwiceWithOneMessageIdAsync(Rebus.Bus.IBus bus)
    {
        var sharedMessageId = $"duplicate-{Guid.NewGuid()}";
        var headers = new Dictionary<string, string> { [Headers.MessageId] = sharedMessageId };

        await bus.SendLocal(new ReproMessage("first"), headers);
        await bus.SendLocal(new ReproMessage("second"), headers);
    }

    private static async Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekQueueAsync(string queue)
    {
        await using var client = new ServiceBusClient(Emulator.ConnectionString);
        return await client.CreateReceiver(queue).PeekMessagesAsync(10);
    }

    private static async Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekDeadLetterQueueAsync(string queue)
    {
        await using var client = new ServiceBusClient(Emulator.ConnectionString);
        var receiver = client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        return await receiver.PeekMessagesAsync(10);
    }

    private static async Task DrainQueueAsync(string queue)
    {
        await DrainAsync(queue, new ServiceBusReceiverOptions());
        await DrainAsync(queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
    }

    private static async Task DrainAsync(string queue, ServiceBusReceiverOptions options)
    {
        await using var client = new ServiceBusClient(Emulator.ConnectionString);
        var receiver = client.CreateReceiver(queue, options);

        while (true)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
            if (message == null)
            {
                return;
            }

            await receiver.CompleteMessageAsync(message);
        }
    }
}

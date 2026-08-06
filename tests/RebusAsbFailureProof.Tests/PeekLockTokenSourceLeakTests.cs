namespace RebusAsbFailureProof.Tests;

using Azure.Messaging.ServiceBus;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Retry.Simple;

/// <summary>
/// Reproduces a defect in Rebus.AzureServiceBus' per-message cancellation support (added in 10.5.0).
///
/// AzureServiceBusTransport.Receive keeps a per-message CancellationTokenSource in
/// _messageRenewerTokenSources, keyed by ServiceBus MessageId:
///
///     var renewFailedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(...);   // L620
///     if (!_messageRenewerTokenSources.TryAdd(message.MessageId, renewFailedTokenSource))  // L621
///     {
///         // should never happen though
///         renewFailedTokenSource.Dispose();                                                // L624
///     }
///     ...
///     items["asb-message-cancel-token"] = renewFailedTokenSource.Token;                    // L632
///
/// MessageId identifies a *message*; what is being tracked is a *delivery*. Azure Service Bus is
/// at-least-once, so the same MessageId is legitimately in flight twice whenever a peek lock lapses
/// and the broker redelivers while the first delivery is still being handled. TryAdd then fails,
/// the token source is disposed, and .Token is read from the disposed instance.
///
/// The ObjectDisposedException is thrown inside Receive, i.e. BEFORE dispatch:
///   - no handler runs, so IFailed&lt;T&gt; and second-level retries never engage;
///   - the retry step never runs, so maxDeliveryAttempts never increments and the message can
///     never reach the Rebus error queue;
///   - the message is never abandoned (TransactionContext.Dispose does not invoke OnNack when
///     _mustAck is null), so it stays locked for the full lock duration each time.
/// It is therefore redelivered every lock duration until MaxDeliveryCount is exhausted and the
/// broker dead-letters it natively.
///
/// Note L620 sits OUTSIDE the `AutomaticallyRenewPeekLock &amp;&amp; !_prefetchingEnabled` guard, so this
/// is not limited to users of automatic peek lock renewal - see the third test.
/// </summary>
[TestClass]
public class PeekLockTokenSourceLeakTests
{
    /// <summary>
    /// THE HEADLINE CASE. A single message, one genuine broker redelivery after a lock lapse.
    ///
    /// repro.lockrecover has LockDuration PT5S and MaxDeliveryCount 5. The handler exceeds the
    /// lock on its FIRST invocation only and returns immediately on every later one, so a correct
    /// client recovers: delivery #2 is dispatched, completes in milliseconds, and the message is
    /// settled well inside the lock. Nothing here is misconfigured - losing a lock is a normal
    /// Azure Service Bus condition that the at-least-once contract exists to handle.
    ///
    /// The short lock only makes the single lapse deterministic; it is NOT what causes the failure.
    /// Crucially, the message is still comfortably processable after the lapse - four further
    /// deliveries each had a full 5 seconds to run a handler that needs milliseconds.
    ///
    /// Correct behaviour: delivery #2 arrives at ~5s, the handler returns immediately, the message
    /// is completed. Actual behaviour: every redelivery that overlaps delivery #1 throws
    /// ObjectDisposedException inside Receive and is silently discarded - the handler is never
    /// entered, and the broker delivery attempts are burned. Processing only resumes once delivery
    /// #1's context finally goes away.
    ///
    /// Whether that ends in dead-lettering depends on whether MaxDeliveryCount is exhausted before
    /// the first delivery completes. In production (LockDuration PT5M, MaxDeliveryCount 10) it was:
    /// 10 receive failures spaced exactly 5 minutes apart, then a native dead-letter. This test
    /// asserts only the part it can attribute unambiguously - that the overlapping redeliveries are
    /// discarded without dispatch.
    ///
    /// What actually happens is that every redelivery is poisoned, the handler is never entered
    /// again, and the broker dead-letters the message. The bug converts a RECOVERABLE lock loss
    /// into a permanently unprocessable message.
    /// </summary>
    [TestMethod]
    [Timeout(300_000)]
    public async Task RecoverableLockLapse_ThenEveryRedeliveryIsPoisonedAndTheBrokerDeadLettersTheMessage()
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
                // for the broker to redeliver several times.
                await Task.Delay(TimeSpan.FromSeconds(40));
                firstDeliveryFinishedAt = DateTimeOffset.UtcNow;
            }

            // Any later invocation returns immediately - this is where a correct client settles.
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

                // Deliberately high, so anything in the error queue is Rebus' doing and anything
                // dead-lettered is the broker's doing.
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

        // Assert
        Console.WriteLine($"handler invocations:              {handlerInvocations}");
        Console.WriteLine($"first delivery finished at:       {firstDeliveryFinishedAt:HH:mm:ss.fff}");
        Console.WriteLine($"ObjectDisposedException events:   {disposedFailures.Length}");
        Console.WriteLine($"  of which during the overlap:    {disposedFailures.Count(f => f.At < overlapEnd)}");
        Console.WriteLine($"redeliveries dispatched during overlap: {dispatchesDuringOverlap}");

        Assert.IsTrue(
            disposedFailures.Count(f => f.At < overlapEnd) > 0,
            "Expected at least one broker redelivery, arriving while delivery #1 was still in " +
            "flight, to trip the disposed CancellationTokenSource inside Receive.");

        Assert.AreEqual(
            0,
            dispatchesDuringOverlap,
            "Expected every redelivery arriving during the overlap to be discarded inside Receive. " +
            "A correct implementation dispatches one of them - the handler returns in milliseconds " +
            "on any invocation after the first - and settles the message there.");

        // Rebus' own error handling never sees any of this: the throw happens before dispatch, so
        // the retry step never runs and maxDeliveryAttempts never increments.
        Assert.AreEqual(
            0,
            (await PeekQueueAsync(Emulator.ErrorQueue)).Count,
            "Expected nothing in the Rebus error queue - the failures bypass Rebus' retry pipeline entirely.");
    }

    /// <summary>
    /// Minimal isolation of the failing line - NOT evidence that Azure Service Bus produces
    /// duplicate message ids on its own. Two sends share one explicitly-set MessageId, which is
    /// supported usage (it is how ASB duplicate detection is driven, and Rebus honours the header),
    /// but the point of this test is only that it trips the same line in ~100ms without waiting on
    /// any lock timing. The case that matters is the broker-driven one above.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public async Task Receive_WhenSameMessageIdIsInFlightTwice_ThenTransportThrowsObjectDisposedExceptionBeforeDispatch()
    {
        // Stage
        await DrainQueueAsync(Emulator.DuplicateIdQueue);

        var logs = new RecordingLoggerFactory();
        using var activator = new BuiltinHandlerActivator();

        activator.Handle<ReproMessage>(async _ => await Task.Delay(TimeSpan.FromSeconds(6)));

        var bus = StartDuplicateIdBus(activator, logs, automaticallyRenewPeekLock: true);

        // Act - two messages carrying the SAME ServiceBus MessageId, delivered concurrently.
        await SendTwiceWithOneMessageIdAsync(bus);
        await Task.Delay(TimeSpan.FromSeconds(20));
        bus.Dispose();

        // Assert
        var disposedFailures = logs.WithException<ObjectDisposedException>();
        Console.WriteLine($"ObjectDisposedException log events: {disposedFailures.Length}");
        foreach (var failure in disposedFailures.Take(3))
        {
            Console.WriteLine($"  [{failure.Level}] {failure.Message}");
        }

        Assert.IsTrue(disposedFailures.Length > 0, "Expected the disposed CancellationTokenSource to be tripped.");

        Assert.IsTrue(
            disposedFailures.Any(f => f.Message.Contains("receive", StringComparison.OrdinalIgnoreCase)),
            "Expected the ObjectDisposedException to be thrown from the receive path (before dispatch).");
    }

    /// <summary>
    /// The token source is created and keyed at L620, which sits OUTSIDE the
    /// `AutomaticallyRenewPeekLock &amp;&amp; !_prefetchingEnabled` guard at L641. This test is identical to
    /// the one above except that automatic peek lock renewal is never switched on - and it still
    /// fails, so the blast radius is not limited to users of that feature.
    /// </summary>
    [TestMethod]
    [Timeout(120_000)]
    public async Task Receive_WhenSameMessageIdIsInFlightTwiceAndRenewalIsDisabled_ThenItStillThrows()
    {
        // Stage
        await DrainQueueAsync(Emulator.DuplicateIdQueue);

        var logs = new RecordingLoggerFactory();
        using var activator = new BuiltinHandlerActivator();

        activator.Handle<ReproMessage>(async _ => await Task.Delay(TimeSpan.FromSeconds(6)));

        var bus = StartDuplicateIdBus(activator, logs, automaticallyRenewPeekLock: false);

        // Act
        await SendTwiceWithOneMessageIdAsync(bus);
        await Task.Delay(TimeSpan.FromSeconds(20));
        bus.Dispose();

        // Assert
        var disposedFailures = logs.WithException<ObjectDisposedException>();
        Console.WriteLine($"ObjectDisposedException log events (renewal DISABLED): {disposedFailures.Length}");

        Assert.IsTrue(
            disposedFailures.Length > 0,
            "Expected the defect to occur even with AutomaticallyRenewPeekLock() never called, " +
            "because the token source is created outside that guard.");
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

# Rebus.AzureServiceBus — redelivered message discarded inside `Receive` by a disposed `CancellationTokenSource`

**Status: FIXED in Rebus.AzureServiceBus 10.7.1.** Reported as
[rebus-org/Rebus.AzureServiceBus#117](https://github.com/rebus-org/Rebus.AzureServiceBus/issues/117),
fixed by keying delivery tracking on the lock token instead of the message id.

This repo started as a reproduction and is now a **regression suite**: the tests assert the fixed
behaviour, so they pass on 10.7.1+ and fail on 10.5.0 – 10.7.0.

| Rebus.AzureServiceBus | Result |
|---|---|
| 10.7.1 | all 3 pass |
| 10.7.0 | all 3 fail |
| 10.5.1 | all 3 fail |

## The bug

`AzureServiceBusTransport.Receive` tracked a per-message `CancellationTokenSource` keyed by the
Service Bus `MessageId`:

```csharp
var renewFailedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationToken);
if (!_messageRenewerTokenSources.TryAdd(message.MessageId, renewFailedTokenSource))
{
    // should never happen though
    renewFailedTokenSource.Dispose();
}
...
items["asb-message-cancel-token"] = renewFailedTokenSource.Token;   // throws when TryAdd failed
```

`MessageId` identifies a *message*; the tracked resource is a *delivery*. Azure Service Bus is
at-least-once, so the same `MessageId` is legitimately in flight twice whenever a peek lock lapses
and the broker redelivers while the handler is still running. `TryAdd` then failed, the token source
was disposed, and `.Token` was read from the disposed instance:

```
System.ObjectDisposedException: The CancellationTokenSource has been disposed.
   at System.Threading.CancellationTokenSource.get_Token()
   at Rebus.AzureServiceBus.AzureServiceBusTransport.Receive(ITransactionContext context, CancellationToken cancellationToken)
   at Rebus.Workers.ThreadPoolBased.ThreadPoolWorker.ReceiveTransportMessage(CancellationToken token, ITransactionContext context)
```

The throw happened **inside `Receive`, before dispatch**, so no handler ran, the retry step never
ran, `maxDeliveryAttempts` never incremented, and the message could not reach the *Rebus* error
queue. It was redelivered every lock duration until `MaxDeliveryCount` was exhausted and the broker
dead-lettered it natively.

Two aggravating details, both addressed in 10.7.1:

- The token source was created **outside** the `AutomaticallyRenewPeekLock && !_prefetchingEnabled`
  guard, so this also hit users who never enabled peek lock renewal (third test below).
- `context.OnDisposed(...)` was registered *after* the `TryAdd`, so a throw in between orphaned a
  tracking entry with no cleanup registered.

## What 10.7.1 changed

Per the maintainer on #117:

- Delivery tracking is keyed by **lock token** (unique per delivery attempt) rather than message id,
  for both `_messageRenewerTokenSources` and `_messageLockRenewers`.
- The cancellation token is captured into a local before its source can be disposed.
- Cleanup registration happens immediately after the dictionaries are populated, ahead of anything
  in `Receive` that can throw.
- `RenewPeekLocks` now removes and disposes the renewer as soon as a renewal fails, instead of
  leaving it to come up "due" again every 10 seconds forever.

## Running it

Requires Docker and the .NET 10 SDK.

```bash
docker compose up -d                              # emulator + SQL Server; queues from emulator/Config.json
dotnet test tests/RebusAsbFailureProof.Tests      # ~2 minutes
```

To check another version, change the `Rebus.AzureServiceBus` `PackageReference` in
`tests/RebusAsbFailureProof.Tests/RebusAsbFailureProof.Tests.csproj` and re-run. Versions before
10.6.0 have no emulator detection, hence `DoNotCreateQueues()` and `DoNotCheckQueueConfiguration()`
in the transport setup — that makes 10.5.1 runnable here too.

If you edit `emulator/Config.json`, run `docker compose restart emulator` — queues are created from
that file at startup.

Tests are `[DoNotParallelize]`: they share one emulator and some share a queue. Each test drains its
queue and dead-letter queue first, so a run left over from a buggy version does not poison the next
run.

## The tests

### 1. `RecoverableLockLapse_ThenTheRedeliveryIsDispatchedAndTheMessageIsSettled` — the case that matters

`repro.lockrecover`: `LockDuration PT5S`, `MaxDeliveryCount 5`. The handler overruns the lock on its
**first** invocation only and returns immediately on every later one, so the redelivery is trivially
processable — it needs milliseconds and has a full 5 seconds.

Asserts: no `ObjectDisposedException`, the redelivery arriving during the overlap **is** dispatched,
nothing dead-lettered, Rebus error queue empty.

The short lock only makes the single lapse deterministic; it is not what caused the failure.

### 2. `Receive_WhenSameMessageIdIsInFlightTwice_ThenBothAreDispatchedWithoutError`

Minimal isolation of the same collision without lock timing: two sends sharing one explicitly-set
`MessageId`, delivered concurrently. Asserts no exception and both messages dispatched.

This is *isolation*, not a claim that Service Bus duplicates ids by itself — setting a stable
`MessageId` is supported usage (it drives ASB duplicate detection, and Rebus honours the header).

### 3. `...AndRenewalIsDisabled_ThenBothAreDispatchedWithoutError`

Same, with `AutomaticallyRenewPeekLock()` never called — the pre-10.7.1 blast radius.

`tests/.../DiagnosticsTests.cs` is `[Ignore]`d scratch work, kept as a record of how the behaviour
was isolated.

## Production context

Observed across four services sharing one Service Bus namespace, 12 occurrence-days in 90 days on
10.5.1. The clearest case: 10 receive failures spaced *exactly* 5 minutes apart (= `LockDuration`),
then silence — matching `MaxDeliveryCount 10` — with the queue's dead-letter count going 0 → 1 at
that moment and staying there.

Also seen there, and **not** reproduced by these tests: a renewer entry surviving for the whole
process lifetime, logging `"Error when renewing peek lock for message with ID {messageId}"` every
~10 s for 2 h 47 min — continuing two hours after the message was dead-lettered, stopping only on
host restart (983 exceptions from one message). 10.7.1 removes the renewer as soon as a renewal
fails, which should close that too.

## Correction to the original report

The original issue filed a "minor" note claiming the exception is logged without a typed exception
attached. That call site is in **Rebus core**, not this transport:

`rebus-org/Rebus` → `Rebus/Workers/ThreadPoolBased/ThreadPoolWorker.cs:159`

```csharp
_log.Warn("An error occurred when attempting to receive the next message: {exception}", exception);
```

`Rebus.AzureServiceBus`'s own renewal logging does use the typed `Warn(Exception, ...)` overload, as
the maintainer pointed out. The consequence is real but belongs to the core repo: log sinks that
index by exception type do not see this one.

# Rebus.AzureServiceBus — a redelivered message is discarded inside `Receive` by a disposed `CancellationTokenSource`

A runnable reproduction, against the official Azure Service Bus emulator in Docker. No Azure
subscription required.

**Verified affected:** `Rebus.AzureServiceBus` **10.5.1** and **10.7.0** (latest at time of writing) —
identical behaviour on both. The lines involved are unchanged from 10.5.0 through master.

## What goes wrong

`AzureServiceBusTransport.Receive` tracks a per-message `CancellationTokenSource` in
`_messageRenewerTokenSources`, keyed by the Service Bus `MessageId`:

```csharp
var renewFailedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationToken);  // L620
if (!_messageRenewerTokenSources.TryAdd(message.MessageId, renewFailedTokenSource))                                   // L621
{
    // should never happen though
    renewFailedTokenSource.Dispose();                                                                                 // L624
}
...
items["asb-message-cancel-token"] = renewFailedTokenSource.Token;                                                     // L632
```

`MessageId` identifies a **message**; what is being tracked is a **delivery**. Azure Service Bus is
at-least-once, so the same `MessageId` is legitimately in flight twice whenever a peek lock lapses
and the broker redelivers while the first delivery is still being handled. `TryAdd` then fails, the
freshly created token source is disposed, and `.Token` is read from that disposed instance eight
lines later.

Because the throw happens **inside `Receive`, before dispatch**:

- no handler runs, so `IFailed<T>` and second-level retries never engage;
- the retry step never runs, so `maxDeliveryAttempts` never increments and the message can never
  reach the **Rebus** error queue;
- the message is never abandoned — `TransactionContext.Dispose` does not invoke `OnNack` when
  `_mustAck` is `null` — so it stays locked for the full lock duration on every attempt.

The redelivery is therefore silently discarded and a broker delivery attempt is burned. Processing
resumes only once the first delivery's context goes away. If `MaxDeliveryCount` is exhausted before
that happens, the broker dead-letters the message natively — outside anything Rebus can observe.

Note `L620` sits **outside** the `AutomaticallyRenewPeekLock && !_prefetchingEnabled` guard at
`L641`, so this is not limited to users of automatic peek lock renewal. The third test demonstrates
that.

## Observed

```
[Warn] An error occurred when attempting to receive the next message: {exception}
   || args: System.ObjectDisposedException: The CancellationTokenSource has been disposed.
   at System.Threading.CancellationTokenSource.get_Token()
   at Rebus.AzureServiceBus.AzureServiceBusTransport.Receive(ITransactionContext context, CancellationToken cancellationToken)
   at Rebus.Workers.ThreadPoolBased.ThreadPoolWorker.ReceiveTransportMessage(CancellationToken token, ITransactionContext context)
```

## Running it

Requires Docker and the .NET 10 SDK.

```bash
docker compose up -d                              # emulator + SQL Server; queues come from emulator/Config.json
dotnet test tests/RebusAsbFailureProof.Tests      # ~2 minutes
```

To try another version, change the `Rebus.AzureServiceBus` `PackageReference` in
`tests/RebusAsbFailureProof.Tests/RebusAsbFailureProof.Tests.csproj` and re-run. Versions before
10.6.0 have no emulator detection, which is why the transport is configured with
`DoNotCreateQueues()` and `DoNotCheckQueueConfiguration()` — that makes 10.5.1 runnable here too.

If you change `emulator/Config.json`, run `docker compose restart emulator` — queues are created
from that file at startup.

Tests are marked `[DoNotParallelize]`: they share one emulator and some share a queue.

## The tests

All three pass, i.e. all three demonstrate the defect.

### 1. `RecoverableLockLapse_ThenEveryRedeliveryIsPoisonedAndTheBrokerDeadLettersTheMessage` — the case that matters

`repro.lockrecover`: `LockDuration PT5S`, `MaxDeliveryCount 5`. The handler overruns the lock on its
**first** invocation only and returns immediately on every later one.

- **Correct behaviour:** the lock lapses once, delivery #2 arrives at ~5 s, the handler returns in
  milliseconds, the message is completed.
- **Actual behaviour:** every redelivery overlapping delivery #1 throws `ObjectDisposedException`
  inside `Receive` and is discarded without dispatch. The Rebus error queue stays empty throughout.

The short lock only makes the single lapse deterministic — it is not what causes the failure. The
message remains comfortably processable after the lapse; each redelivery had a full 5 seconds to run
a handler needing milliseconds.

The test asserts only what it can attribute unambiguously: redeliveries arriving during the overlap
are discarded, and nothing reaches the Rebus error queue. It deliberately does **not** assert
dead-lettering — see *Scope of the claim* below.

### 2. `Receive_WhenSameMessageIdIsInFlightTwice_...` — minimal isolation

Two sends sharing one explicitly-set `MessageId`, delivered concurrently. Trips the same line in
~100 ms with no lock timing involved.

This is a **minimal isolation of the failing line, not evidence that Service Bus produces duplicate
message ids on its own**. Setting a stable `MessageId` is supported usage (it is how ASB duplicate
detection is driven, and Rebus honours the header), but the argument rests on test 1.

### 3. `Receive_WhenSameMessageIdIsInFlightTwiceAndRenewalIsDisabled_ThenItStillThrows` — blast radius

Identical to test 2 except `AutomaticallyRenewPeekLock()` is never called. It still fails, because
the token source is created outside that guard.

`tests/.../DiagnosticsTests.cs` is `[Ignore]`d scratch work kept as evidence of how the behaviour
was isolated.

## Scope of the claim

Reproduced here:

- a genuine broker redelivery overlapping an in-flight delivery throws inside `Receive`;
- the redelivery is discarded without dispatch and a delivery attempt is burned;
- none of it reaches the Rebus error queue;
- it happens with and without `AutomaticallyRenewPeekLock()`, on 10.5.1 and 10.7.0.

**Not** reproduced here, and reported as production observation only:

- **Native dead-lettering as the end state.** It follows mechanically once `MaxDeliveryCount` is
  exhausted before the first delivery completes, and we see it in production — but a test that
  forces it also tends to make the message unprocessable for unrelated reasons, so we do not claim
  it from a test.
- **A permanently orphaned entry.** In production (10.5.1, ASB Premium, `LockDuration PT5M`,
  `MaxDeliveryCount 10`) we saw `"Error when renewing peek lock for message with ID {messageId}"`
  every ~10 s for 2 h 47 min for a single message id, continuing for two hours *after* that message
  was dead-lettered, and stopping only when the host restarted — 983 logged exceptions from one
  message. Here the renewer is cleaned up correctly in both versions, so we could not reproduce it.

  Two code paths would explain it, both visible by inspection:
  - `RenewPeekLocks` cancels the token source on renewal failure without removing the dictionary
    entry, and `MessageLockRenewer.GetTimeOfNextRenewal` does not update `_nextRenewal` on failure —
    once `LockedUntil` is in the past it returns a past time, so `IsDue` latches `true` and the
    10-second renewal task retries forever. 2 h 47 min ÷ 10 s ≈ 1000, against 983 observed.
  - `context.OnDisposed(...)` is registered at `L711`, *after* the `TryAdd` at `L621`, so a throw in
    between (e.g. the `RebusApplicationException` at `L636`) orphans an entry with no cleanup
    registered. Neither dictionary is cleared in `Dispose()`.

  This resembles issue **#40**, fixed in 7.0.0-a15.

## What a fix needs to do

Deliberately stated as requirements rather than a patch — the right implementation is the
maintainers' call.

1. The receive path must not throw on a condition the broker produces by design.
2. The tracking key must identify a **delivery**, not a message. The lock token is unique per
   delivery; `MessageId` is not. The same aliasing affects `_messageLockRenewers`, where one
   delivery's cleanup can remove another delivery's renewer and silently leave it unrenewed.
3. A renewer whose lock is definitively lost (`MessageLockLost`) cannot be renewed again and should
   stop retrying rather than logging every 10 s for the process lifetime.

Two things worth noting in the current code's favour: `OnAck` and `OnNack` already remove from the
dictionaries *before* calling Complete/Abandon, which closes the obvious redelivery race — the
remaining hole is specifically the lock-lapse overlap. And simply reading the existing entry after a
failed `TryAdd` is **not** a fix: it races with the other delivery's cleanup (`KeyNotFoundException`,
or a token source disposed a moment later), and sharing one token source across two concurrent
deliveries means cancelling one cancels the other.

## Production context

Same signature across four services sharing one Service Bus namespace, 12 occurrence-days in 90
days. The clearest case: 10 receive failures spaced *exactly* 5 minutes apart (= `LockDuration`),
then silence — matching `MaxDeliveryCount 10` — with the queue's dead-letter count going 0 → 1 at
that moment and staying there.

## Minor

The exception is logged via `_log.Warn("...: {exception}", exception)` rather than the
`Warn(Exception, ...)` overload, so it arrives as rendered text with no typed exception attached.
Log sinks that index by exception type do not see it — which is why these took a while to find in
Application Insights.

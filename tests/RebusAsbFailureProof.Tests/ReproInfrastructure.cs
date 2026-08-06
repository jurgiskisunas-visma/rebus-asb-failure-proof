namespace RebusAsbFailureProof.Tests;

using System.Collections.Concurrent;
using Rebus.Logging;

/// <summary>Connection details for the Service Bus emulator started by docker-compose.yml.</summary>
public static class Emulator
{
    public const string ConnectionString =
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    public const string DuplicateIdQueue = "repro.dupid";
    public const string LockLossQueue = "repro.lockloss";

    /// <summary>LockDuration PT5S, MaxDeliveryCount 5 - a lock lapse a correct client recovers from.</summary>
    public const string LockRecoverQueue = "repro.lockrecover";
    public const string ErrorQueue = "error";
}

public sealed record LogEvent(LogLevel Level, string Message, Exception Exception, DateTimeOffset At);

/// <summary>
/// Captures everything Rebus logs so the tests can assert on transport-level failures
/// that Rebus swallows into its own log rather than surfacing to the caller.
/// </summary>
public sealed class RecordingLoggerFactory : IRebusLoggerFactory
{
    private readonly ConcurrentQueue<LogEvent> events = new();

    public LogEvent[] Events => this.events.ToArray();

    public ILog GetLogger<T>() => new RecordingLog(this.events);

    /// <summary>
    /// Rebus logs some transport failures by passing the exception as a *format argument*
    /// (template "...: {exception}") rather than through the ILog overload that takes an
    /// Exception, so matching on the rendered text as well as the typed exception is required.
    /// </summary>
    public LogEvent[] WithException<TException>()
        where TException : Exception =>
        this.Events
            .Where(e => e.Exception is TException || e.Message.Contains(typeof(TException).Name, StringComparison.Ordinal))
            .ToArray();

    public LogEvent[] Matching(string fragment) =>
        this.Events.Where(e => e.Message.Contains(fragment, StringComparison.OrdinalIgnoreCase)).ToArray();

    private sealed class RecordingLog(ConcurrentQueue<LogEvent> events) : ILog
    {
        public void Debug(string message, params object[] objs) => this.Add(LogLevel.Debug, message, objs, null);

        public void Info(string message, params object[] objs) => this.Add(LogLevel.Info, message, objs, null);

        public void Warn(string message, params object[] objs) => this.Add(LogLevel.Warn, message, objs, null);

        public void Warn(Exception exception, string message, params object[] objs) =>
            this.Add(LogLevel.Warn, message, objs, exception);

        public void Error(string message, params object[] objs) => this.Add(LogLevel.Error, message, objs, null);

        public void Error(Exception exception, string message, params object[] objs) =>
            this.Add(LogLevel.Error, message, objs, exception);

        private void Add(LogLevel level, string message, object[] objs, Exception exception)
        {
            var rendered = message;
            if (objs?.Length > 0)
            {
                rendered = message + " || args: " + string.Join(", ", objs.Select(o => o?.ToString() ?? "<null>"));
            }

            events.Enqueue(new LogEvent(level, rendered, exception, DateTimeOffset.UtcNow));
        }
    }
}

public sealed record ReproMessage(string Label);

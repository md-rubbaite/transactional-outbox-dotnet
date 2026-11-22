using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Outbox;

public sealed class OutboxRelayOptions
{
    /// <summary>Messages claimed per round. Default 100.</summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>
    /// How long a claim is exclusive. Must comfortably exceed one batch's publish time; if a relay
    /// crashes, its messages are picked up after this. Default 30 seconds.
    /// </summary>
    public TimeSpan Lease { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Publishes are cancelled this long before the lease ends, so we never publish without a lease. Default 5 seconds.</summary>
    public TimeSpan LeaseSafetyMargin { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Wait between polls when the outbox is empty. Default 1 second.</summary>
    public TimeSpan IdleDelay { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Messages of different partition keys published in parallel. Default 8.</summary>
    public int MaxParallelism { get; init; } = 8;

    public RetryPolicy Retry { get; init; } = new();

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxParallelism, 1);
        if (LeaseSafetyMargin >= Lease) throw new ArgumentException("LeaseSafetyMargin must be shorter than Lease.");
    }
}

/// <summary>
/// Moves messages from the outbox to the broker. Run one or more instances; they coordinate through
/// leases in the store. Delivery is at least once: a crash between publish and MarkSent republishes
/// the message, so consumers must be idempotent (see <see cref="IdempotentConsumer"/>).
/// </summary>
public sealed class OutboxRelay
{
    public const string TelemetryName = "Outbox.Relay";
    private static readonly ActivitySource Activities = new(TelemetryName);
    private static readonly Meter Meter = new(TelemetryName);
    private static readonly Counter<long> Published = Meter.CreateCounter<long>("outbox.messages.published");
    private static readonly Counter<long> Failed = Meter.CreateCounter<long>("outbox.messages.failed");
    private static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>("outbox.messages.dead_lettered");
    private static readonly Counter<long> LeaseLost = Meter.CreateCounter<long>("outbox.leases.lost");
    private static readonly Histogram<double> Lag = Meter.CreateHistogram<double>("outbox.publish.lag", "s", "Time from OccurredAt to broker acknowledgement");

    private readonly IOutboxStore _store;
    private readonly IMessagePublisher _publisher;
    private readonly OutboxRelayOptions _options;
    private readonly TimeProvider _time;

    public OutboxRelay(IOutboxStore store, IMessagePublisher publisher, OutboxRelayOptions? options = null, TimeProvider? time = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _options = options ?? new OutboxRelayOptions();
        _options.Validate();
        _time = time ?? TimeProvider.System;
    }

    /// <summary>Claim and process one batch. Returns the number of messages claimed.</summary>
    public async Task<int> RunOnceAsync(CancellationToken cancellationToken = default)
    {
        var batch = await _store.ClaimAsync(_options.BatchSize, _options.Lease, cancellationToken).ConfigureAwait(false);
        if (batch.Count == 0) return 0;
        await Parallel.ForEachAsync(
            batch,
            new ParallelOptions { MaxDegreeOfParallelism = _options.MaxParallelism, CancellationToken = cancellationToken },
            async (leased, ct) => await DeliverAsync(leased, ct).ConfigureAwait(false)).ConfigureAwait(false);
        return batch.Count;
    }

    /// <summary>Run until cancelled. Store errors are retried after the idle delay.</summary>
    public async Task RunAsync(Action<Exception>? onError = null, CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            int claimed;
            try
            {
                claimed = await RunOnceAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
#pragma warning disable CA1031 // the loop must survive transient database failures
            catch (Exception ex)
#pragma warning restore CA1031
            {
                onError?.Invoke(ex);
                claimed = 0;
            }
            if (claimed == 0)
            {
                try
                {
                    await Task.Delay(_options.IdleDelay, _time, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private async Task DeliverAsync(LeasedMessage leased, CancellationToken cancellationToken)
    {
        var message = leased.Message;
        using var activity = Activities.StartActivity("outbox publish", ActivityKind.Producer, ParentContext(message));
        activity?.SetTag("messaging.message.id", message.Id);
        activity?.SetTag("messaging.message.type", message.Type);
        activity?.SetTag("outbox.attempt", leased.Attempt);

        var budget = leased.LeasedUntil - _options.LeaseSafetyMargin - _time.GetUtcNow();
        if (budget <= TimeSpan.Zero)
        {
            return; // lease nearly gone before we started; let it expire and be reclaimed
        }

        Exception? error = null;
        using (var leaseCts = new CancellationTokenSource(budget, _time))
        using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, leaseCts.Token))
        {
            try
            {
                await _publisher.PublishAsync(message, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw; // shutting down: the lease will expire and another relay retries
            }
#pragma warning disable CA1031 // every publisher failure is recorded on the message
            catch (Exception ex)
#pragma warning restore CA1031
            {
                error = ex;
            }
        }

        if (error is null)
        {
            if (await _store.MarkSentAsync(leased, CancellationToken.None).ConfigureAwait(false))
            {
                Published.Add(1, new KeyValuePair<string, object?>("type", message.Type));
                Lag.Record((_time.GetUtcNow() - message.OccurredAt).TotalSeconds);
            }
            else
            {
                LeaseLost.Add(1);
            }
            return;
        }

        activity?.SetStatus(ActivityStatusCode.Error, error.Message);
        var delay = error is PermanentPublishException ? null : _options.Retry.NextDelay(leased.Attempt);
        var retryAt = delay is { } d ? _time.GetUtcNow() + d : (DateTimeOffset?)null;
        var description = $"{error.GetType().Name}: {error.Message}";
        if (description.Length > 2000) description = description[..2000];
        if (!await _store.MarkFailedAsync(leased, description, retryAt, CancellationToken.None).ConfigureAwait(false))
        {
            LeaseLost.Add(1);
            return;
        }
        if (retryAt is null) DeadLettered.Add(1, new KeyValuePair<string, object?>("type", message.Type));
        else Failed.Add(1, new KeyValuePair<string, object?>("type", message.Type));
    }

    private static ActivityContext ParentContext(OutboxMessage message) =>
        message.Headers.TryGetValue("traceparent", out var traceparent)
        && ActivityContext.TryParse(traceparent, message.Headers.GetValueOrDefault("tracestate"), out var context)
            ? context
            : default;
}

using System.Collections.Concurrent;
using Outbox.InMemory;

namespace Outbox.Tests;

internal sealed class ManualClock(DateTimeOffset start) : TimeProvider
{
    private long _ticks = start.UtcTicks;
    public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);
    public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
}

/// <summary>Records publishes; behaviour per call is decided by a function.</summary>
internal sealed class TestPublisher(Func<OutboxMessage, int, CancellationToken, Task>? behaviour = null) : IMessagePublisher
{
    private readonly ConcurrentDictionary<Guid, int> _calls = new();
    private readonly ConcurrentDictionary<string, int> _inFlight = new();
    public ConcurrentQueue<OutboxMessage> Acknowledged { get; } = new();
    public int MaxConcurrentPerPartition;

    public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        var call = _calls.AddOrUpdate(message.Id, 1, (_, n) => n + 1);
        var concurrent = _inFlight.AddOrUpdate(message.PartitionKey, 1, (_, n) => n + 1);
        InterlockedMax(ref MaxConcurrentPerPartition, concurrent);
        try
        {
            if (behaviour is not null) await behaviour(message, call, cancellationToken).ConfigureAwait(false);
            await Task.Yield();
            Acknowledged.Enqueue(message);
        }
        finally
        {
            _inFlight.AddOrUpdate(message.PartitionKey, 0, (_, n) => n - 1);
        }
    }

    public int Calls(Guid id) => _calls.GetValueOrDefault(id);

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, current) != current) { }
    }
}

internal static class Db
{
    public static readonly DateTimeOffset T0 = new(2026, 11, 22, 9, 0, 0, TimeSpan.Zero);

    /// <summary>Place an order: the business row and its event in one transaction.</summary>
    public static async Task<OutboxMessage> PlaceOrderAsync(InMemoryDatabase db, string orderId, string partition, TimeProvider time, string? body = null)
    {
        var tx = db.Begin();
        await using (tx)
        {
            tx.Put("orders", orderId, body ?? $"{{\"id\":\"{orderId}\"}}");
            var message = OutboxMessage.Create("order.placed.v1", $"{{\"orderId\":\"{orderId}\"}}", partition, time.GetUtcNow());
            tx.Enqueue(message);
            await tx.CommitAsync(time);
            return message;
        }
    }
}

using System.Collections.Concurrent;
using Outbox;
using Outbox.InMemory;

// 1. The bug: publish first, then commit. The commit fails and the event is already out.
var naiveDb = new InMemoryDatabase { CrashBeforeCommit = () => true };
var broker = new FlakyBroker(failureRate: 0);
var naiveTx = naiveDb.Begin();
naiveTx.Put("orders", "o-100", "{}");
await broker.PublishAsync(OutboxMessage.Create("order.placed.v1", "{}", "o-100", DateTimeOffset.UtcNow), default);
try { await naiveTx.CommitAsync(CancellationToken.None); } catch (SimulatedCrashException) { }
Console.WriteLine($"naive dual write : order saved = {naiveDb.Get("orders", "o-100") is not null}, events published = {broker.Delivered.Count}  <- phantom event");

// 2. The fix: the event is a row in the same transaction.
var db = new InMemoryDatabase();
var crashNext = true;
db.CrashBeforeCommit = () => crashNext;
try { await PlaceOrder(db, "o-101", "customer-1"); } catch (SimulatedCrashException) { }
crashNext = false;
Console.WriteLine($"outbox, crash    : order saved = {db.Get("orders", "o-101") is not null}, outbox rows = {db.OutboxRows().Count}  <- consistent");

// 3. 300 orders for 10 customers, two relays, a broker that fails 30% of the time.
for (var i = 0; i < 300; i++) await PlaceOrder(db, $"o-{i}", $"customer-{i % 10}");
var flaky = new FlakyBroker(failureRate: 0.3);
var options = new OutboxRelayOptions { Retry = new RetryPolicy(maxAttempts: 100, baseDelay: TimeSpan.FromMilliseconds(1), maxDelay: TimeSpan.FromMilliseconds(5)) };
var relays = new[] { new OutboxRelay(new InMemoryOutboxStore(db), flaky, options), new OutboxRelay(new InMemoryOutboxStore(db), flaky, options) };
while (db.OutboxRows().Any(r => r.Status == OutboxStatus.Pending))
{
    await Task.WhenAll(relays.Select(r => r.RunOnceAsync()));
    await Task.Delay(5);
}
var inOrder = flaky.Delivered.GroupBy(m => m.PartitionKey).All(g => g.Select(m => int.Parse(m.Payload, System.Globalization.CultureInfo.InvariantCulture)).SequenceEqual(g.Select(m => int.Parse(m.Payload, System.Globalization.CultureInfo.InvariantCulture)).Order()));
Console.WriteLine($"flaky broker     : {flaky.Delivered.Count} delivered after {flaky.Failures} failed attempts; per-customer order kept = {inOrder}");

// 4. The broker redelivers; the consumer applies each message once.
var billing = new InMemoryDatabase();
var consumer = new IdempotentConsumer<InMemoryTransaction>("billing", _ => Task.FromResult(billing.Begin()));
var applied = 0;
foreach (var message in flaky.Delivered.Concat(flaky.Delivered.Take(50)))
{
    if (await consumer.HandleAsync(message, (tx, _) => { tx.Put("invoices", message.Id.ToString(), "{}"); return Task.CompletedTask; })) applied++;
}
Console.WriteLine($"idempotent inbox : {flaky.Delivered.Count + 50} deliveries, {applied} invoices created");

static async Task PlaceOrder(InMemoryDatabase db, string orderId, string customer)
{
    var tx = db.Begin();
    await using (tx)
    {
        tx.Put("orders", orderId, "{}");
        var n = orderId.StartsWith("o-", StringComparison.Ordinal) && int.TryParse(orderId[2..], out var x) ? x : -1;
        tx.Enqueue(OutboxMessage.Create("order.placed.v1", n.ToString(System.Globalization.CultureInfo.InvariantCulture), customer, DateTimeOffset.UtcNow));
        await tx.CommitAsync(CancellationToken.None);
    }
}

internal sealed class FlakyBroker(double failureRate) : IMessagePublisher
{
    private readonly object _gate = new();
    private readonly Random _random = new(1);
    public ConcurrentQueue<OutboxMessage> Delivered { get; } = new();
    public int Failures;

    public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        bool fail;
        lock (_gate) fail = _random.NextDouble() < failureRate;
        if (fail)
        {
            Interlocked.Increment(ref Failures);
            throw new IOException("broker unavailable");
        }
        Delivered.Enqueue(message);
        return Task.CompletedTask;
    }
}

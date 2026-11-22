using Outbox.InMemory;

namespace Outbox.Tests;

public class AtomicityTests
{
    [Fact]
    public async Task Naive_dual_write_publishes_an_event_for_an_order_that_never_existed()
    {
        // The anti-pattern this repository replaces: publish, then commit.
        var db = new InMemoryDatabase { CrashBeforeCommit = () => true };
        var publisher = new TestPublisher();
        var tx = db.Begin();
        tx.Put("orders", "o-1", "{}");
        await publisher.PublishAsync(OutboxMessage.Create("order.placed.v1", "{}", "o-1", Db.T0), default);
        await Assert.ThrowsAsync<SimulatedCrashException>(() => tx.CommitAsync(CancellationToken.None));
        Assert.Null(db.Get("orders", "o-1"));
        Assert.Single(publisher.Acknowledged); // downstream systems now act on a phantom order
    }

    [Fact]
    public async Task With_the_outbox_a_crash_before_commit_leaves_neither_order_nor_event()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase { CrashBeforeCommit = () => true };
        await Assert.ThrowsAsync<SimulatedCrashException>(() => Db.PlaceOrderAsync(db, "o-1", "o-1", clock));
        Assert.Null(db.Get("orders", "o-1"));
        Assert.Empty(db.OutboxRows());
        var publisher = new TestPublisher();
        Assert.Equal(0, await new OutboxRelay(new InMemoryOutboxStore(db, clock), publisher, time: clock).RunOnceAsync());
        Assert.Empty(publisher.Acknowledged);
    }

    [Fact]
    public async Task A_committed_order_is_published_exactly_once_and_marked_sent()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        var message = await Db.PlaceOrderAsync(db, "o-1", "o-1", clock);
        var publisher = new TestPublisher();
        var relay = new OutboxRelay(new InMemoryOutboxStore(db, clock), publisher, time: clock);
        Assert.Equal(1, await relay.RunOnceAsync());
        Assert.Equal(0, await relay.RunOnceAsync());
        Assert.Equal(message.Id, Assert.Single(publisher.Acknowledged).Id);
        var row = Assert.Single(db.OutboxRows());
        Assert.Equal(OutboxStatus.Sent, row.Status);
        Assert.Equal(1, row.Attempts);
    }
}

public class OrderingTests
{
    [Fact]
    public async Task A_failing_message_blocks_later_messages_of_its_key_but_not_other_keys()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        var a1 = await Db.PlaceOrderAsync(db, "a1", "customer-A", clock);
        var a2 = await Db.PlaceOrderAsync(db, "a2", "customer-A", clock);
        var b1 = await Db.PlaceOrderAsync(db, "b1", "customer-B", clock);
        var publisher = new TestPublisher((m, call, _) =>
            m.Id == a1.Id && call == 1 ? throw new TimeoutException("broker timeout") : Task.CompletedTask);
        var options = new OutboxRelayOptions { Retry = new RetryPolicy(baseDelay: TimeSpan.FromSeconds(10), random: () => 0.5) };
        var relay = new OutboxRelay(new InMemoryOutboxStore(db, clock), publisher, options, clock);

        await relay.RunOnceAsync(); // a1 fails, b1 succeeds, a2 waits behind a1
        Assert.Equal([b1.Id], publisher.Acknowledged.Select(m => m.Id).ToList());
        Assert.Equal(0, publisher.Calls(a2.Id));
        Assert.Equal(0, await relay.RunOnceAsync()); // a1 not due yet, a2 still blocked

        clock.Advance(TimeSpan.FromSeconds(6)); // backoff = 0.5 * 10 s
        await relay.RunOnceAsync(); // a1
        await relay.RunOnceAsync(); // a2
        Assert.Equal([b1.Id, a1.Id, a2.Id], publisher.Acknowledged.Select(m => m.Id).ToList());
    }

    [Fact]
    public async Task Concurrent_relays_with_a_flaky_broker_deliver_everything_in_key_order()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        var written = new List<OutboxMessage>();
        var rnd = new Random(7);
        for (var i = 0; i < 400; i++) written.Add(await Db.PlaceOrderAsync(db, $"o{i}", $"customer-{rnd.Next(20)}", clock));
        var flaky = new Random(11);
        var gate = new object();
        var publisher = new TestPublisher((_, _, _) =>
        {
            bool fail;
            lock (gate) fail = flaky.NextDouble() < 0.2;
            return fail ? throw new IOException("connection reset") : Task.CompletedTask;
        });
        var options = new OutboxRelayOptions { BatchSize = 7, MaxParallelism = 4, Retry = new RetryPolicy(maxAttempts: 50, baseDelay: TimeSpan.FromMilliseconds(1), maxDelay: TimeSpan.FromMilliseconds(5)) };
        var relays = Enumerable.Range(0, 4).Select(_ => new OutboxRelay(new InMemoryOutboxStore(db, clock), publisher, options, clock)).ToList();

        for (var round = 0; round < 2000 && db.OutboxRows().Any(r => r.Status == OutboxStatus.Pending); round++)
        {
            await Task.WhenAll(relays.Select(r => Task.Run(() => r.RunOnceAsync())));
            clock.Advance(TimeSpan.FromMilliseconds(10));
        }

        Assert.All(db.OutboxRows(), r => Assert.Equal(OutboxStatus.Sent, r.Status));
        Assert.Equal(1, publisher.MaxConcurrentPerPartition);
        var acked = publisher.Acknowledged.ToList();
        Assert.Equal(400, acked.Count);
        foreach (var group in written.GroupBy(m => m.PartitionKey))
        {
            Assert.Equal(group.Select(m => m.Id).ToList(), acked.Where(m => m.PartitionKey == group.Key).Select(m => m.Id).ToList());
        }
    }
}

public class FailureTests
{
    [Fact]
    public async Task Transient_failures_back_off_then_dead_letter_after_the_last_attempt()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        var message = await Db.PlaceOrderAsync(db, "o-1", "o-1", clock);
        var publisher = new TestPublisher((_, _, _) => throw new TimeoutException("broker down"));
        var options = new OutboxRelayOptions { Retry = new RetryPolicy(maxAttempts: 3, baseDelay: TimeSpan.FromSeconds(1), random: () => 0.99) };
        var store = new InMemoryOutboxStore(db, clock);
        var relay = new OutboxRelay(store, publisher, options, clock);
        for (var i = 0; i < 5; i++)
        {
            await relay.RunOnceAsync();
            clock.Advance(TimeSpan.FromMinutes(1));
        }
        Assert.Equal(3, publisher.Calls(message.Id));
        var row = Assert.Single(db.OutboxRows());
        Assert.Equal(OutboxStatus.DeadLettered, row.Status);
        Assert.Contains("broker down", row.LastError);

        Assert.True(store.Requeue(message.Id));
        var healthy = new OutboxRelay(store, new TestPublisher(), options, clock);
        Assert.Equal(1, await healthy.RunOnceAsync());
        Assert.Equal(OutboxStatus.Sent, Assert.Single(db.OutboxRows()).Status);
    }

    [Fact]
    public async Task A_permanent_failure_is_dead_lettered_at_once()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        await Db.PlaceOrderAsync(db, "o-1", "o-1", clock);
        var publisher = new TestPublisher((_, _, _) => throw new PermanentPublishException("schema rejected"));
        await new OutboxRelay(new InMemoryOutboxStore(db, clock), publisher, time: clock).RunOnceAsync();
        var row = Assert.Single(db.OutboxRows());
        Assert.Equal(OutboxStatus.DeadLettered, row.Status);
        Assert.Equal(1, row.Attempts);
    }

    [Fact]
    public async Task A_crashed_relay_keeps_its_lease_until_expiry_and_cannot_complete_afterwards()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        await Db.PlaceOrderAsync(db, "o-1", "o-1", clock);
        var store = new InMemoryOutboxStore(db, clock);

        var crashed = Assert.Single(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default)); // then the process dies
        Assert.Empty(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default));

        clock.Advance(TimeSpan.FromSeconds(31));
        var publisher = new TestPublisher();
        Assert.Equal(1, await new OutboxRelay(store, publisher, time: clock).RunOnceAsync());
        Assert.Equal(2, Assert.Single(db.OutboxRows()).Attempts);

        // the "dead" relay wakes up: its stale lease token is refused
        Assert.False(await store.MarkSentAsync(crashed, default));
        Assert.False(await store.MarkFailedAsync(crashed, "late", null, default));
        Assert.Equal(OutboxStatus.Sent, Assert.Single(db.OutboxRows()).Status);
    }

    [Fact]
    public async Task A_stale_lease_cannot_complete_a_message_another_relay_now_holds()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        await Db.PlaceOrderAsync(db, "o-1", "o-1", clock);
        var store = new InMemoryOutboxStore(db, clock);
        var slow = Assert.Single(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default));
        clock.Advance(TimeSpan.FromSeconds(31));
        var current = Assert.Single(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default));

        Assert.False(await store.MarkFailedAsync(slow, "late failure", null, default)); // would dead-letter it
        Assert.False(await store.MarkSentAsync(slow, default));
        Assert.Equal(OutboxStatus.Pending, Assert.Single(db.OutboxRows()).Status);
        Assert.True(await store.MarkSentAsync(current, default));
    }

    [Fact]
    public async Task A_publish_that_outlives_its_lease_is_cancelled_and_retried()
    {
        var clock = new ManualClock(Db.T0);
        var db = new InMemoryDatabase();
        var message = await Db.PlaceOrderAsync(db, "o-1", "o-1", clock);
        var publisher = new TestPublisher(async (_, call, ct) =>
        {
            if (call == 1) await Task.Delay(Timeout.Infinite, ct); // the broker never answers
        });
        var options = new OutboxRelayOptions
        {
            Lease = TimeSpan.FromMilliseconds(300),
            LeaseSafetyMargin = TimeSpan.FromMilliseconds(100),
            Retry = new RetryPolicy(random: () => 0),
        };
        var relay = new OutboxRelay(new InMemoryOutboxStore(db, clock), publisher, options, clock);
        await relay.RunOnceAsync();
        var row = Assert.Single(db.OutboxRows());
        Assert.Equal(OutboxStatus.Pending, row.Status);
        Assert.Contains("Canceled", row.LastError);
        await relay.RunOnceAsync();
        Assert.Equal(2, publisher.Calls(message.Id));
    }

    [Fact]
    public void Backoff_grows_exponentially_with_jitter_and_stops_at_the_last_attempt()
    {
        var policy = new RetryPolicy(maxAttempts: 4, baseDelay: TimeSpan.FromSeconds(1), maxDelay: TimeSpan.FromSeconds(3), random: () => 0.999999);
        Assert.Equal(1, Math.Round(policy.NextDelay(1)!.Value.TotalSeconds));
        Assert.Equal(2, Math.Round(policy.NextDelay(2)!.Value.TotalSeconds));
        Assert.Equal(3, Math.Round(policy.NextDelay(3)!.Value.TotalSeconds)); // capped
        Assert.Null(policy.NextDelay(4));
    }
}

public class InboxTests
{
    private static IdempotentConsumer<InMemoryTransaction> Consumer(InMemoryDatabase db) =>
        new("billing", _ => Task.FromResult(db.Begin()));

    [Fact]
    public async Task Redelivered_messages_are_applied_once()
    {
        var db = new InMemoryDatabase();
        var consumer = Consumer(db);
        var message = OutboxMessage.Create("order.placed.v1", "{}", "o-1", Db.T0);
        var applied = 0;
        Task Handler(InMemoryTransaction tx, CancellationToken _)
        {
            applied++;
            tx.Put("invoices", "o-1", "{}");
            return Task.CompletedTask;
        }
        Assert.True(await consumer.HandleAsync(message, Handler));
        Assert.False(await consumer.HandleAsync(message, Handler));
        Assert.False(await consumer.HandleAsync(message, Handler));
        Assert.Equal(1, applied);
        Assert.True(db.InboxContains("billing", message.Id));
    }

    [Fact]
    public async Task A_handler_that_fails_records_nothing_so_redelivery_retries_it()
    {
        var db = new InMemoryDatabase();
        var consumer = Consumer(db);
        var message = OutboxMessage.Create("order.placed.v1", "{}", "o-1", Db.T0);
        await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.HandleAsync(message, (tx, _) =>
        {
            tx.Put("invoices", "o-1", "{}");
            throw new InvalidOperationException("tax service down");
        }));
        Assert.False(db.InboxContains("billing", message.Id));
        Assert.Null(db.Get("invoices", "o-1"));
        Assert.True(await consumer.HandleAsync(message, (tx, _) =>
        {
            tx.Put("invoices", "o-1", "{}");
            return Task.CompletedTask;
        }));
        Assert.NotNull(db.Get("invoices", "o-1"));
    }

    [Fact]
    public async Task Two_concurrent_deliveries_of_one_message_commit_once()
    {
        var db = new InMemoryDatabase();
        var consumer = Consumer(db);
        var message = OutboxMessage.Create("order.placed.v1", "{}", "o-1", Db.T0);
        var bothInside = new TaskCompletionSource();
        var arrived = 0;
        var charges = 0;
        async Task Handler(InMemoryTransaction tx, CancellationToken _)
        {
            if (Interlocked.Increment(ref arrived) == 2) bothInside.SetResult();
            await bothInside.Task; // force the race: both pass the inbox check before either commits
            tx.Put("ledger", $"charge-{Guid.NewGuid()}", "{}");
            Interlocked.Increment(ref charges);
        }
        var results = await Task.WhenAll(consumer.HandleAsync(message, Handler), consumer.HandleAsync(message, Handler));
        Assert.Equal(1, results.Count(r => r));
        Assert.Equal(2, charges); // both handlers ran...
        Assert.Equal(1, db.Count("ledger")); // ...but only one transaction committed
    }
}

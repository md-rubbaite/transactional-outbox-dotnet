using Npgsql;

namespace Outbox.Postgres.Tests;

/// <summary>Runs only when OUTBOX_PG_CS is set (CI starts a PostgreSQL service container).</summary>
public sealed class PostgresFactAttribute : FactAttribute
{
    public PostgresFactAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("OUTBOX_PG_CS")))
        {
            Skip = "set OUTBOX_PG_CS to a PostgreSQL connection string to run";
        }
    }
}

// One class, so the tests run sequentially against the shared tables.
public sealed class PostgresOutboxTests : IAsyncLifetime
{
    private NpgsqlDataSource _ds = null!;

    public async Task InitializeAsync()
    {
        var cs = Environment.GetEnvironmentVariable("OUTBOX_PG_CS");
        if (string.IsNullOrEmpty(cs)) return;
        _ds = NpgsqlDataSource.Create(cs);
        await PostgresOutbox.EnsureSchemaAsync(_ds);
        await using var cmd = _ds.CreateCommand(
            "create table if not exists orders (id text primary key, body text not null); truncate orders, outbox_message, inbox_message");
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (_ds is not null) await _ds.DisposeAsync();
    }

    private async Task<OutboxMessage> PlaceOrderAsync(string orderId, string partition, bool commit = true)
    {
        await using var conn = await _ds.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using (var cmd = new NpgsqlCommand("insert into orders (id, body) values (@id, '{}')", conn, tx))
        {
            cmd.Parameters.AddWithValue("id", orderId);
            await cmd.ExecuteNonQueryAsync();
        }
        var message = OutboxMessage.Create("order.placed.v1", $"{{\"orderId\":\"{orderId}\"}}", partition, DateTimeOffset.UtcNow,
            new Dictionary<string, string> { ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" });
        await PostgresOutbox.EnqueueAsync(conn, tx, message);
        if (commit) await tx.CommitAsync();
        else await tx.RollbackAsync();
        return message;
    }

    private async Task<long> ScalarAsync(string sql)
    {
        await using var cmd = _ds.CreateCommand(sql);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    [PostgresFact]
    public async Task Rollback_leaves_no_message_commit_publishes_once_with_headers()
    {
        await PlaceOrderAsync("rolled-back", "p0", commit: false);
        var placed = await PlaceOrderAsync("o-1", "p1");
        Assert.Equal(1, await ScalarAsync("select count(*) from outbox_message"));

        var store = new PostgresOutboxStore(_ds);
        var published = new List<OutboxMessage>();
        var relay = new OutboxRelay(store, new DelegatePublisher(m => { published.Add(m); return Task.CompletedTask; }));
        Assert.Equal(1, await relay.RunOnceAsync());
        Assert.Equal(0, await relay.RunOnceAsync());
        var got = Assert.Single(published);
        Assert.Equal(placed.Id, got.Id);
        Assert.Equal(placed.Headers["traceparent"], got.Headers["traceparent"]);
        Assert.Equal(1, await ScalarAsync("select count(*) from outbox_message where status = 1"));
    }

    [PostgresFact]
    public async Task Per_key_order_holds_and_stale_leases_are_fenced()
    {
        var a1 = await PlaceOrderAsync("a1", "key-A");
        var a2 = await PlaceOrderAsync("a2", "key-A");
        var b1 = await PlaceOrderAsync("b1", "key-B");
        var store = new PostgresOutboxStore(_ds);

        var first = await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default);
        Assert.Equal([a1.Id, b1.Id], first.Select(m => m.Message.Id).ToList()); // a2 waits behind a1
        Assert.Empty(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default));

        Assert.True(await store.MarkFailedAsync(first[0], "timeout", DateTimeOffset.UtcNow.AddMinutes(-1), default));
        Assert.True(await store.MarkSentAsync(first[1], default));
        Assert.False(await store.MarkSentAsync(first[0], default)); // lease already released

        var second = Assert.Single(await store.ClaimAsync(10, TimeSpan.FromMilliseconds(200), default));
        Assert.Equal(a1.Id, second.Message.Id);
        Assert.Equal(2, second.Attempt);
        await Task.Delay(400); // lease expires: a crashed relay
        var third = Assert.Single(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default));
        Assert.False(await store.MarkSentAsync(second, default)); // stale token refused
        Assert.True(await store.MarkSentAsync(third, default));

        var fourth = Assert.Single(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default));
        Assert.Equal(a2.Id, fourth.Message.Id);
        Assert.True(await store.MarkFailedAsync(fourth, "schema rejected", null, default));
        Assert.Equal(1, await ScalarAsync("select count(*) from outbox_message where status = 2"));
        Assert.True(await store.RequeueAsync(a2.Id));
        Assert.Single(await store.ClaimAsync(10, TimeSpan.FromSeconds(30), default));
    }

    [PostgresFact]
    public async Task Concurrent_relays_deliver_everything_without_double_leases()
    {
        var rnd = new Random(3);
        for (var i = 0; i < 200; i++) await PlaceOrderAsync($"c{i}", $"k{rnd.Next(15)}");
        var inFlight = new System.Collections.Concurrent.ConcurrentDictionary<Guid, int>();
        var violations = 0;
        var publisher = new DelegatePublisher(async m =>
        {
            if (inFlight.AddOrUpdate(m.Id, 1, (_, n) => n + 1) > 1) Interlocked.Increment(ref violations);
            await Task.Delay(1);
            inFlight.AddOrUpdate(m.Id, 0, (_, n) => n - 1);
        });
        var options = new OutboxRelayOptions { BatchSize = 10, MaxParallelism = 4 };
        var relays = Enumerable.Range(0, 4).Select(_ => new OutboxRelay(new PostgresOutboxStore(_ds), publisher, options)).ToList();
        for (var round = 0; round < 500 && await ScalarAsync("select count(*) from outbox_message where status = 0") > 0; round++)
        {
            await Task.WhenAll(relays.Select(r => r.RunOnceAsync()));
        }
        Assert.Equal(0, violations);
        Assert.Equal(0, await ScalarAsync("select count(*) from outbox_message where status <> 1"));
        Assert.Equal(200, await ScalarAsync("select count(*) from outbox_message where status = 1"));
    }

    [PostgresFact]
    public async Task Inbox_detects_a_concurrent_duplicate_delivery()
    {
        var id = Guid.NewGuid();
        await using var first = await PostgresInboxTransaction.BeginAsync(_ds);
        Assert.True(await first.TryRecordAsync("billing", id, default));
        await using var second = await PostgresInboxTransaction.BeginAsync(_ds);
        var waiting = second.TryRecordAsync("billing", id, default);
        await Task.Delay(300);
        Assert.False(waiting.IsCompleted); // blocked on the primary key until the first one ends
        await first.CommitAsync(default);
        Assert.False(await waiting);
    }

    private sealed class DelegatePublisher(Func<OutboxMessage, Task> publish) : IMessagePublisher
    {
        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken) => publish(message);
    }
}

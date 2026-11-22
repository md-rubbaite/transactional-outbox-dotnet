namespace Outbox.InMemory;

/// <summary>
/// Outbox store over <see cref="InMemoryDatabase"/>. Same claim rules as the PostgreSQL store:
/// only the oldest unsent message of each partition key is eligible, and only when it is due and
/// not leased by a live relay.
/// </summary>
public sealed class InMemoryOutboxStore(InMemoryDatabase db, TimeProvider? time = null) : IOutboxStore
{
    private readonly TimeProvider _time = time ?? TimeProvider.System;

    public Task<IReadOnlyList<LeasedMessage>> ClaimAsync(int batchSize, TimeSpan lease, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        var now = _time.GetUtcNow();
        var claimed = new List<LeasedMessage>();
        lock (db.Gate)
        {
            var heads = db.Outbox
                .Where(r => r.Status == OutboxStatus.Pending)
                .GroupBy(r => r.Message.PartitionKey, StringComparer.Ordinal)
                .Select(g => g.MinBy(r => r.Sequence)!)
                .Where(r => (r.LockedUntil is null || r.LockedUntil < now) && r.NextAttemptAt <= now)
                .OrderBy(r => r.Sequence)
                .Take(batchSize)
                .ToList();
            foreach (var row in heads)
            {
                row.Attempts += 1;
                row.LeaseToken = Guid.NewGuid();
                row.LockedUntil = now + lease;
                claimed.Add(new LeasedMessage(row.Message, row.Sequence, row.Attempts, row.LeaseToken.Value, row.LockedUntil.Value));
            }
        }
        return Task.FromResult<IReadOnlyList<LeasedMessage>>(claimed);
    }

    public Task<bool> MarkSentAsync(LeasedMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (db.Gate)
        {
            var row = Holding(message);
            if (row is null) return Task.FromResult(false);
            row.Status = OutboxStatus.Sent;
            row.SentAt = _time.GetUtcNow();
            row.LockedUntil = null;
            row.LeaseToken = null;
            return Task.FromResult(true);
        }
    }

    public Task<bool> MarkFailedAsync(LeasedMessage message, string failure, DateTimeOffset? retryAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (db.Gate)
        {
            var row = Holding(message);
            if (row is null) return Task.FromResult(false);
            row.LastError = failure;
            row.LockedUntil = null;
            row.LeaseToken = null;
            if (retryAt is { } at) row.NextAttemptAt = at;
            else row.Status = OutboxStatus.DeadLettered;
            return Task.FromResult(true);
        }
    }

    /// <summary>Put a dead-lettered message back in line, after fixing the cause.</summary>
    public bool Requeue(Guid messageId)
    {
        lock (db.Gate)
        {
            var row = db.Outbox.FirstOrDefault(r => r.Message.Id == messageId && r.Status == OutboxStatus.DeadLettered);
            if (row is null) return false;
            row.Status = OutboxStatus.Pending;
            row.NextAttemptAt = _time.GetUtcNow();
            return true;
        }
    }

    private InMemoryDatabase.OutboxRow? Holding(LeasedMessage message) =>
        db.Outbox.FirstOrDefault(r =>
            r.Sequence == message.Sequence && r.Status == OutboxStatus.Pending && r.LeaseToken == message.LeaseToken);
}

namespace Outbox.InMemory;

/// <summary>
/// A tiny transactional database for tests, demos and prototypes: key-value business tables, an
/// outbox table and an inbox table, with atomic commit. It reproduces the guarantees the
/// PostgreSQL implementation relies on, in process.
/// </summary>
public sealed class InMemoryDatabase
{
    internal readonly object Gate = new();
    internal readonly Dictionary<(string Table, string Key), string> Rows = [];
    internal readonly List<OutboxRow> Outbox = [];
    internal readonly HashSet<(string Consumer, Guid MessageId)> Inbox = [];
    internal long NextSequence = 1;

    /// <summary>When this returns true, the next commit fails with <see cref="SimulatedCrashException"/>, for fault-injection tests.</summary>
    public Func<bool>? CrashBeforeCommit { get; set; }

    public InMemoryTransaction Begin() => new(this);

    public string? Get(string table, string key)
    {
        lock (Gate) return Rows.GetValueOrDefault((table, key));
    }

    /// <summary>Number of rows in a business table.</summary>
    public int Count(string table)
    {
        lock (Gate) return Rows.Keys.Count(k => k.Table == table);
    }

    public IReadOnlyList<OutboxRowSnapshot> OutboxRows()
    {
        lock (Gate) return Outbox.Select(r => r.Snapshot()).ToList();
    }

    public bool InboxContains(string consumer, Guid messageId)
    {
        lock (Gate) return Inbox.Contains((consumer, messageId));
    }

    internal sealed class OutboxRow(OutboxMessage message, long sequence, DateTimeOffset availableAt)
    {
        public OutboxMessage Message { get; } = message;
        public long Sequence { get; } = sequence;
        public OutboxStatus Status { get; set; } = OutboxStatus.Pending;
        public int Attempts { get; set; }
        public DateTimeOffset NextAttemptAt { get; set; } = availableAt;
        public DateTimeOffset? LockedUntil { get; set; }
        public Guid? LeaseToken { get; set; }
        public string? LastError { get; set; }
        public DateTimeOffset? SentAt { get; set; }

        public OutboxRowSnapshot Snapshot() => new(Message, Sequence, Status, Attempts, NextAttemptAt, LockedUntil, LastError, SentAt);
    }
}

public sealed record OutboxRowSnapshot(
    OutboxMessage Message,
    long Sequence,
    OutboxStatus Status,
    int Attempts,
    DateTimeOffset NextAttemptAt,
    DateTimeOffset? LockedUntil,
    string? LastError,
    DateTimeOffset? SentAt);

/// <summary>Simulated process crash.</summary>
public sealed class SimulatedCrashException : Exception
{
    public SimulatedCrashException() : base("simulated crash") { }
    public SimulatedCrashException(string message) : base(message) { }
    public SimulatedCrashException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Staged writes, applied together on commit or not at all. Business rows, outbox messages and
/// inbox records written here become visible atomically.
/// </summary>
public sealed class InMemoryTransaction : IInboxTransaction
{
    private readonly InMemoryDatabase _db;
    private readonly Dictionary<(string, string), string> _rows = [];
    private readonly List<OutboxMessage> _outbox = [];
    private readonly HashSet<(string, Guid)> _inbox = [];
    private bool _done;

    internal InMemoryTransaction(InMemoryDatabase db) => _db = db;

    public void Put(string table, string key, string value)
    {
        EnsureOpen();
        _rows[(table, key)] = value;
    }

    /// <summary>Add a message to the outbox as part of this transaction.</summary>
    public void Enqueue(OutboxMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        EnsureOpen();
        _outbox.Add(message);
    }

    public Task<bool> TryRecordAsync(string consumer, Guid messageId, CancellationToken cancellationToken)
    {
        EnsureOpen();
        lock (_db.Gate)
        {
            if (_db.Inbox.Contains((consumer, messageId))) return Task.FromResult(false);
        }
        return Task.FromResult(_inbox.Add((consumer, messageId)));
    }

    public Task CommitAsync(CancellationToken cancellationToken) => CommitAsync(TimeProvider.System);

    public Task CommitAsync(TimeProvider time)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(time);
        if (_db.CrashBeforeCommit?.Invoke() == true)
        {
            _done = true;
            throw new SimulatedCrashException();
        }
        lock (_db.Gate)
        {
            if (_inbox.Any(_db.Inbox.Contains))
            {
                _done = true;
                throw new InboxConflictException("message already processed by a concurrent delivery");
            }
            foreach (var (k, v) in _rows) _db.Rows[k] = v;
            var now = time.GetUtcNow();
            foreach (var m in _outbox) _db.Outbox.Add(new InMemoryDatabase.OutboxRow(m, _db.NextSequence++, now));
            _db.Inbox.UnionWith(_inbox);
        }
        _done = true;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _done = true; // uncommitted writes are discarded
        return ValueTask.CompletedTask;
    }

    private void EnsureOpen() => ObjectDisposedException.ThrowIf(_done, this);
}

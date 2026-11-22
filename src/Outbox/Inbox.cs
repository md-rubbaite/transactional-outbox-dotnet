namespace Outbox;

/// <summary>A consumer-side database transaction that can record processed message ids.</summary>
public interface IInboxTransaction : IAsyncDisposable
{
    /// <summary>
    /// Record that <paramref name="consumer"/> processed <paramref name="messageId"/>, in this
    /// transaction. Returns false if it was already recorded (a duplicate delivery).
    /// </summary>
    Task<bool> TryRecordAsync(string consumer, Guid messageId, CancellationToken cancellationToken);

    /// <summary>Commit. Throws <see cref="InboxConflictException"/> if a concurrent delivery of the same message committed first.</summary>
    Task CommitAsync(CancellationToken cancellationToken);
}

/// <summary>Another delivery of the same message committed first; this one must be discarded.</summary>
public sealed class InboxConflictException : Exception
{
    public InboxConflictException() { }
    public InboxConflictException(string message) : base(message) { }
    public InboxConflictException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Makes an at-least-once consumer effectively exactly-once: the message id is recorded in the same
/// transaction as the handler's writes, so a redelivered message is detected and skipped, and a
/// crash before commit leaves neither the writes nor the record behind.
/// </summary>
public sealed class IdempotentConsumer<TTransaction> where TTransaction : IInboxTransaction
{
    private readonly string _consumer;
    private readonly Func<CancellationToken, Task<TTransaction>> _begin;

    public IdempotentConsumer(string consumer, Func<CancellationToken, Task<TTransaction>> begin)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        _consumer = consumer;
        _begin = begin ?? throw new ArgumentNullException(nameof(begin));
    }

    /// <summary>
    /// Run <paramref name="handler"/> once per message id. Returns false for a duplicate. If the
    /// handler throws, nothing is committed and the broker should redeliver.
    /// </summary>
    public async Task<bool> HandleAsync(
        OutboxMessage message,
        Func<TTransaction, CancellationToken, Task> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(handler);
        var tx = await _begin(cancellationToken).ConfigureAwait(false);
        await using (tx.ConfigureAwait(false))
        {
            if (!await tx.TryRecordAsync(_consumer, message.Id, cancellationToken).ConfigureAwait(false)) return false;
            await handler(tx, cancellationToken).ConfigureAwait(false);
            try
            {
                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (InboxConflictException)
            {
                return false;
            }
            return true;
        }
    }
}

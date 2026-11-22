namespace Outbox;

/// <summary>Durable outbox, read side, used by the relay.</summary>
public interface IOutboxStore
{
    /// <summary>
    /// Lease up to <paramref name="batchSize"/> messages that are ready: pending, due, not leased,
    /// and first in line for their partition key. Safe to call from many relays at once.
    /// </summary>
    Task<IReadOnlyList<LeasedMessage>> ClaimAsync(int batchSize, TimeSpan lease, CancellationToken cancellationToken);

    /// <summary>Record a successful publish. Returns false if the lease was lost to another relay.</summary>
    Task<bool> MarkSentAsync(LeasedMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// Record a failed publish (<paramref name="failure"/> describes it). With <paramref name="retryAt"/> the message is retried then;
    /// without it, it is dead-lettered. Returns false if the lease was lost.
    /// </summary>
    Task<bool> MarkFailedAsync(LeasedMessage message, string failure, DateTimeOffset? retryAt, CancellationToken cancellationToken);
}

/// <summary>Sends a message to the broker (Kafka, RabbitMQ, Azure Service Bus, SNS, a webhook).</summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publish and wait for the broker's acknowledgement. Throw on failure. Throw
    /// <see cref="PermanentPublishException"/> for failures a retry cannot fix.
    /// </summary>
    Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken);
}

/// <summary>A publish failure that retrying cannot fix (rejected schema, unknown topic, payload too large).</summary>
public sealed class PermanentPublishException : Exception
{
    public PermanentPublishException() { }
    public PermanentPublishException(string message) : base(message) { }
    public PermanentPublishException(string message, Exception innerException) : base(message, innerException) { }
}

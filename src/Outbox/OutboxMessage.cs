namespace Outbox;

/// <summary>
/// An event or command recorded in the same database transaction as the business change that
/// caused it.
/// </summary>
/// <param name="Id">Stable identity. Consumers deduplicate on it, so it must never change on retry.</param>
/// <param name="Type">Message type name, for example <c>order.placed.v1</c>.</param>
/// <param name="Payload">Serialized body, usually JSON.</param>
/// <param name="PartitionKey">
/// Messages with the same key are delivered in the order they were written, one at a time.
/// Use the aggregate id (order id, account id). Different keys are delivered in parallel.
/// </param>
/// <param name="OccurredAt">When the business change happened.</param>
/// <param name="Headers">Trace context, correlation id, tenant, schema version.</param>
public sealed record OutboxMessage(
    Guid Id,
    string Type,
    string Payload,
    string PartitionKey,
    DateTimeOffset OccurredAt,
    IReadOnlyDictionary<string, string> Headers)
{
    public static OutboxMessage Create(
        string type,
        string payload,
        string partitionKey,
        DateTimeOffset occurredAt,
        IReadOnlyDictionary<string, string>? headers = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        return new OutboxMessage(Guid.NewGuid(), type, payload, partitionKey, occurredAt, headers ?? new Dictionary<string, string>());
    }
}

public enum OutboxStatus
{
    Pending = 0,
    Sent = 1,
    DeadLettered = 2,
}

/// <summary>A message claimed by one relay for a limited time.</summary>
/// <param name="Sequence">Position in the outbox, increasing in write order.</param>
/// <param name="Attempt">1 for the first delivery attempt.</param>
/// <param name="LeaseToken">Fencing token: only the holder of the current lease can complete it.</param>
/// <param name="LeasedUntil">After this instant another relay may claim the message.</param>
public sealed record LeasedMessage(OutboxMessage Message, long Sequence, int Attempt, Guid LeaseToken, DateTimeOffset LeasedUntil);

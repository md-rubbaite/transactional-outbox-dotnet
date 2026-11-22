using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text.Json;

namespace Outbox.Postgres;

/// <summary>
/// PostgreSQL outbox. Works with any ADO.NET provider for PostgreSQL; in practice pass an
/// <c>NpgsqlDataSource</c>. Uses the database clock for leases, so relays on hosts with skewed
/// clocks still agree.
/// </summary>
public sealed class PostgresOutboxStore(DbDataSource dataSource) : IOutboxStore
{
    private readonly DbDataSource _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));

    public async Task<IReadOnlyList<LeasedMessage>> ClaimAsync(int batchSize, TimeSpan lease, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        await using var command = _dataSource.CreateCommand(PostgresSql.Claim);
        command.Add("batch_size", batchSize, DbType.Int32);
        command.Add("lease_seconds", lease.TotalSeconds, DbType.Double);
        var result = new List<LeasedMessage>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var headers = JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(6)) ?? [];
            var message = new OutboxMessage(
                reader.GetGuid(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetFieldValue<DateTimeOffset>(5),
                headers);
            result.Add(new LeasedMessage(
                message,
                reader.GetInt64(0),
                reader.GetInt32(7),
                reader.GetGuid(8),
                reader.GetFieldValue<DateTimeOffset>(9)));
        }
        result.Sort((a, b) => a.Sequence.CompareTo(b.Sequence));
        return result;
    }

    public async Task<bool> MarkSentAsync(LeasedMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var command = _dataSource.CreateCommand(PostgresSql.MarkSent);
        command.Add("seq", message.Sequence, DbType.Int64);
        command.Add("lease_token", message.LeaseToken, DbType.Guid);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    public async Task<bool> MarkFailedAsync(LeasedMessage message, string failure, DateTimeOffset? retryAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using var command = _dataSource.CreateCommand(retryAt is null ? PostgresSql.MarkDead : PostgresSql.MarkRetry);
        command.Add("seq", message.Sequence, DbType.Int64);
        command.Add("lease_token", message.LeaseToken, DbType.Guid);
        command.Add("failure", failure, DbType.String);
        if (retryAt is { } at) command.Add("retry_at", at.ToUniversalTime(), DbType.DateTimeOffset);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    /// <summary>Put a dead-lettered message back in line after fixing the cause.</summary>
    public async Task<bool> RequeueAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(PostgresSql.Requeue);
        command.Add("id", messageId, DbType.Guid);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    /// <summary>Delete up to <paramref name="batchSize"/> sent messages older than <paramref name="olderThan"/>. Call repeatedly until it returns 0.</summary>
    public async Task<int> PurgeSentAsync(TimeSpan olderThan, int batchSize = 5000, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(PostgresSql.PurgeSent);
        command.Add("older_than_seconds", olderThan.TotalSeconds, DbType.Double);
        command.Add("batch_size", batchSize, DbType.Int32);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>Write side: enqueue in the caller's own transaction.</summary>
public static class PostgresOutbox
{
    /// <summary>DDL for the outbox and inbox tables. Idempotent.</summary>
    public static string SchemaSql => PostgresSql.Schema;

    /// <summary>
    /// Insert <paramref name="message"/> using the business transaction, so the message exists if
    /// and only if the business change commits.
    /// </summary>
    public static async Task EnqueueAsync(DbConnection connection, DbTransaction transaction, OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(message);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgresSql.Enqueue;
        command.Add("id", message.Id, DbType.Guid);
        command.Add("type", message.Type, DbType.String);
        command.Add("payload", message.Payload, DbType.String);
        command.Add("partition_key", message.PartitionKey, DbType.String);
        command.Add("occurred_at", message.OccurredAt.ToUniversalTime(), DbType.DateTimeOffset);
        command.Add("headers", JsonSerializer.Serialize(message.Headers), DbType.String);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task EnsureSchemaAsync(DbDataSource dataSource, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        await using var command = dataSource.CreateCommand(PostgresSql.Schema);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>A consumer transaction on PostgreSQL that records processed message ids.</summary>
public sealed class PostgresInboxTransaction : IInboxTransaction
{
    private PostgresInboxTransaction(DbConnection connection, DbTransaction transaction)
    {
        Connection = connection;
        Transaction = transaction;
    }

    public DbConnection Connection { get; }
    public DbTransaction Transaction { get; }

    public static async Task<PostgresInboxTransaction> BeginAsync(DbDataSource dataSource, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            return new PostgresInboxTransaction(connection, transaction);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Inserts the inbox row. A concurrent delivery of the same message waits on the primary key until
    /// this transaction ends, then sees the row and reports a duplicate.
    /// </summary>
    public async Task<bool> TryRecordAsync(string consumer, Guid messageId, CancellationToken cancellationToken)
    {
        await using var command = Connection.CreateCommand();
        command.Transaction = Transaction;
        command.CommandText = PostgresSql.InboxRecord;
        command.Add("consumer", consumer, DbType.String);
        command.Add("message_id", messageId, DbType.Guid);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    public Task CommitAsync(CancellationToken cancellationToken) => Transaction.CommitAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        await Transaction.DisposeAsync().ConfigureAwait(false); // rolls back if not committed
        await Connection.DisposeAsync().ConfigureAwait(false);
    }
}

internal static class PostgresSql
{
    public static readonly string Schema = Load("schema");
    public static readonly string Enqueue = Load("enqueue");
    public static readonly string Claim = Load("claim");
    public static readonly string MarkSent = Load("mark_sent");
    public static readonly string MarkRetry = Load("mark_retry");
    public static readonly string MarkDead = Load("mark_dead");
    public static readonly string Requeue = Load("requeue");
    public static readonly string PurgeSent = Load("purge_sent");
    public static readonly string InboxRecord = Load("inbox_record");

    private static string Load(string name)
    {
        using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream($"Outbox.Postgres.Sql.{name}.sql")
            ?? throw new InvalidOperationException($"missing embedded SQL {name}");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    public static void Add(this DbCommand command, string name, object value, DbType type)
    {
        var p = command.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        p.DbType = type;
        command.Parameters.Add(p);
    }
}

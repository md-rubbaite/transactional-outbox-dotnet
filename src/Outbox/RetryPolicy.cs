namespace Outbox;

/// <summary>Exponential backoff with full jitter and a cap on attempts.</summary>
public sealed class RetryPolicy
{
    public RetryPolicy(int maxAttempts = 10, TimeSpan? baseDelay = null, TimeSpan? maxDelay = null, Func<double>? random = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        MaxAttempts = maxAttempts;
        BaseDelay = baseDelay ?? TimeSpan.FromSeconds(1);
        MaxDelay = maxDelay ?? TimeSpan.FromMinutes(15);
        _random = random ?? Random.Shared.NextDouble;
    }

    private readonly Func<double> _random;

    public int MaxAttempts { get; }
    public TimeSpan BaseDelay { get; }
    public TimeSpan MaxDelay { get; }

    /// <summary>Delay before the next attempt, or null when <paramref name="attempt"/> was the last one.</summary>
    public TimeSpan? NextDelay(int attempt)
    {
        if (attempt >= MaxAttempts) return null;
        var exponent = Math.Min(attempt - 1, 30);
        var ceilingTicks = Math.Min(MaxDelay.Ticks, BaseDelay.Ticks * (double)(1L << exponent));
        return TimeSpan.FromTicks((long)(_random() * ceilingTicks));
    }
}

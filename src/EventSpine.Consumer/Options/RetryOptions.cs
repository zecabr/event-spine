namespace EventSpine.Consumer.Options;

/// <summary>
/// Retry policy for events that fail during projection.
///
/// - <see cref="MaxAttempts"/>: total attempts, including the first one.
///   3 = one initial try + two retries; on the third failure, event is sent
///   to the DLQ and the offset is committed.
/// - <see cref="BackoffBaseMs"/>: base delay for exponential backoff. The nth
///   retry waits BackoffBaseMs * 2^(n-1). Default 100 ms → 100, 200, 400.
/// - <see cref="DlqTopicName"/>: Kafka topic where poisoned envelopes are also
///   published (best-effort — the dlq_events table is the source of truth).
/// </summary>
public sealed class RetryOptions
{
    public int MaxAttempts { get; set; } = 3;
    public int BackoffBaseMs { get; set; } = 100;
    public string DlqTopicName { get; set; } = "orders.events.dlq";
    public string MainTopicName { get; set; } = "orders.events";
}

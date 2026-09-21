namespace EventSpine.Consumer.Data;

/// <summary>
/// Idempotency ledger. One row per event_id ever processed by this consumer.
/// Insert-only, no updates. If insert conflicts, event is a replay — skip.
/// </summary>
public sealed class ConsumerInboxEntry
{
    public Guid EventId { get; init; }
    public DateTime ProcessedAt { get; init; }
}

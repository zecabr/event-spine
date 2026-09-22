namespace EventSpine.Consumer.Data;

/// <summary>
/// Dead-letter entry. One row per event that failed processing after the retry
/// budget was exhausted (or that arrived poisoned and could not be deserialized).
///
/// UNIQUE(event_id) makes re-attempts idempotent: a second failure of the same
/// event updates attempts + last_failed_at instead of inserting a duplicate.
/// </summary>
public sealed class DlqEvent
{
    public Guid Id { get; set; }
    public Guid EventId { get; set; }
    public string? EventType { get; set; }
    public Guid? AggregateId { get; set; }

    /// <summary>Raw envelope JSON, exactly as consumed from the topic.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>
    /// Why the event landed here: "projection_failure" (business logic threw
    /// after retries), "poison" (malformed JSON), or a domain-specific reason
    /// added later.
    /// </summary>
    public string Reason { get; set; } = string.Empty;

    public string? ErrorMessage { get; set; }
    public int Attempts { get; set; }
    public DateTime FirstFailedAt { get; set; }
    public DateTime LastFailedAt { get; set; }

    /// <summary>Set when an operator replays the event via POST /dlq/{id}/replay.</summary>
    public DateTime? ReplayedAt { get; set; }
    public int ReplayedCount { get; set; }
}

namespace EventSpine.Consumer.Data;

/// <summary>
/// Materialized read model of an order, derived from the events stream.
/// Version bumps on every event applied — lets clients detect stale reads.
/// </summary>
public sealed class OrderView
{
    public Guid Id { get; init; }
    public decimal Amount { get; set; }
    public string Status { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public long Version { get; set; }
}

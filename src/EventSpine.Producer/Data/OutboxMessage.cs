namespace EventSpine.Producer.Data;

/// <summary>
/// Linha da tabela <c>outbox</c>. Inserida na mesma transaction do agregado
/// (ver ADR-001). O <c>OutboxRelay</c> faz poll de linhas com <c>SentAt IS NULL</c>,
/// publica no Kafka usando <c>AggregateId</c> como key, e marca <c>SentAt</c> após ack.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; init; }
    public string EventType { get; init; } = string.Empty;
    public Guid AggregateId { get; init; }
    public DateTime OccurredAt { get; init; }
    public string Payload { get; init; } = string.Empty;
    public DateTime? SentAt { get; set; }
}

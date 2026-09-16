namespace EventSpine.Contracts.Events;

/// <summary>
/// Wrapper de transporte pro payload dos eventos. Estrutura estável entre versões
/// (mudar isso quebra consumers existentes; a mudança do payload interno é livre
/// desde que o EventType permita disambiguação).
/// </summary>
/// <param name="EventId">Id único do evento — usado como chave de idempotência no Consumer.</param>
/// <param name="EventType">Nome CLR curto do evento (ex.: "OrderCreated"). Deserializador usa pra dispatch.</param>
/// <param name="AggregateId">Id do agregado (Order.Id) — usado como Kafka key pra ordem por chave.</param>
/// <param name="OccurredAt">Timestamp UTC do evento como aconteceu no producer.</param>
/// <param name="Payload">JSON serializado do evento tipado (OrderCreated, OrderPaid, etc).</param>
public sealed record EventEnvelope(
    Guid EventId,
    string EventType,
    Guid AggregateId,
    DateTime OccurredAt,
    string Payload);

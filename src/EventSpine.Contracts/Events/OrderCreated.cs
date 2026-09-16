namespace EventSpine.Contracts.Events;

/// <summary>Ordem criada. Primeiro evento no ciclo de vida de um Order.</summary>
public sealed record OrderCreated(
    Guid OrderId,
    decimal Amount,
    DateTime CreatedAt) : IOrderEvent;

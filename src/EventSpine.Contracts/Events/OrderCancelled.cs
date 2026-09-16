namespace EventSpine.Contracts.Events;

/// <summary>Ordem cancelada.</summary>
public sealed record OrderCancelled(
    Guid OrderId,
    string Reason,
    DateTime CancelledAt) : IOrderEvent;

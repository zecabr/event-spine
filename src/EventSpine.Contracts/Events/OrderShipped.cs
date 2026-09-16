namespace EventSpine.Contracts.Events;

/// <summary>Ordem despachada.</summary>
public sealed record OrderShipped(
    Guid OrderId,
    string TrackingCode,
    DateTime ShippedAt) : IOrderEvent;

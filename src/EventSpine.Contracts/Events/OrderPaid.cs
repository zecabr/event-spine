namespace EventSpine.Contracts.Events;

/// <summary>Pagamento confirmado.</summary>
public sealed record OrderPaid(
    Guid OrderId,
    decimal Amount,
    DateTime PaidAt) : IOrderEvent;

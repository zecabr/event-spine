namespace EventSpine.Producer.Domain;

/// <summary>
/// Agregado Order — modelo transacional. Estado próprio (source of truth) que
/// vive em <c>orders</c>. Cada mudança gera um evento correspondente no outbox
/// (mesma transaction), publicado pelo <c>OutboxRelay</c>.
/// </summary>
public sealed class Order
{
    public Guid Id { get; init; }
    public decimal Amount { get; init; }
    public OrderStatus Status { get; private set; }
    public DateTime CreatedAt { get; init; }
    public DateTime UpdatedAt { get; private set; }

    // ctor pro EF Core / testes
    public Order(Guid id, decimal amount, OrderStatus status, DateTime createdAt, DateTime updatedAt)
    {
        Id = id;
        Amount = amount;
        Status = status;
        CreatedAt = createdAt;
        UpdatedAt = updatedAt;
    }

    public static Order Create(decimal amount, DateTime now)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(amount);
        return new Order(Guid.NewGuid(), amount, OrderStatus.Created, now, now);
    }

    public void MarkPaid(DateTime now)
    {
        if (Status != OrderStatus.Created)
        {
            throw new InvalidOperationException($"Order {Id} não pode ser paga a partir do status {Status}.");
        }

        Status = OrderStatus.Paid;
        UpdatedAt = now;
    }
}

public enum OrderStatus
{
    Created,
    Paid,
    Shipped,
    Cancelled,
}

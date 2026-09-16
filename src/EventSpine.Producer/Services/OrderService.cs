using System.Text.Json;
using EventSpine.Contracts.Events;
using EventSpine.Producer.Data;
using EventSpine.Producer.Domain;
using Microsoft.EntityFrameworkCore;

namespace EventSpine.Producer.Services;

/// <summary>
/// Coração do padrão outbox: cada mutação de agregado é acompanhada por uma
/// entrada correspondente na tabela <c>outbox</c>, ambas dentro da mesma
/// transaction. Se qualquer parte falhar, nada persiste.
/// </summary>
public sealed class OrderService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly EventSpineDbContext _db;
    private readonly TimeProvider _clock;

    public OrderService(EventSpineDbContext db, TimeProvider clock)
    {
        _db = db;
        _clock = clock;
    }

    public async Task<Order> CreateOrderAsync(decimal amount, CancellationToken ct = default)
    {
        var now = _clock.GetUtcNow().UtcDateTime;
        var order = Order.Create(amount, now);
        var evt = new OrderCreated(order.Id, order.Amount, now);

        await using var tx = await _db.Database.BeginTransactionAsync(ct).ConfigureAwait(false);
        _db.Orders.Add(order);
        _db.Outbox.Add(BuildOutboxMessage(evt, now));
        await _db.SaveChangesAsync(ct).ConfigureAwait(false);
        await tx.CommitAsync(ct).ConfigureAwait(false);

        return order;
    }

    public async Task<Order> PayOrderAsync(Guid orderId, CancellationToken ct = default)
    {
        var order = await _db.Orders.FirstOrDefaultAsync(o => o.Id == orderId, ct).ConfigureAwait(false)
            ?? throw new KeyNotFoundException($"Order {orderId} não encontrada.");

        var now = _clock.GetUtcNow().UtcDateTime;
        order.MarkPaid(now);
        var evt = new OrderPaid(order.Id, order.Amount, now);

        await using var tx = await _db.Database.BeginTransactionAsync(ct).ConfigureAwait(false);
        _db.Outbox.Add(BuildOutboxMessage(evt, now));
        await _db.SaveChangesAsync(ct).ConfigureAwait(false);
        await tx.CommitAsync(ct).ConfigureAwait(false);

        return order;
    }

    public Task<Order?> GetOrderAsync(Guid orderId, CancellationToken ct = default) =>
        _db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == orderId, ct);

    private static OutboxMessage BuildOutboxMessage(IOrderEvent evt, DateTime now) => new()
    {
        Id = Guid.NewGuid(),
        EventType = evt.GetType().Name,
        AggregateId = evt.OrderId,
        OccurredAt = now,
        Payload = JsonSerializer.Serialize<object>(evt, JsonOptions),
        SentAt = null,
    };
}

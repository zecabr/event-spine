using EventSpine.Consumer.Data;
using EventSpine.Contracts.Events;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace EventSpine.Consumer.Projection;

/// <summary>
/// Applies an inbound envelope to the read model within a single Postgres
/// transaction, guarded by the consumer_inbox for idempotency.
///
/// Contract:
///   1. INSERT into consumer_inbox — ON CONFLICT DO NOTHING.
///   2. If insert affected zero rows, event is a replay — return, no-op.
///   3. Otherwise, mutate orders_view according to the event type.
///   4. Commit both together — either both apply or neither does.
/// </summary>
public sealed class OrderProjectionService
{
    private readonly EventSpineConsumerDbContext _db;
    private readonly ILogger<OrderProjectionService> _logger;

    public OrderProjectionService(
        EventSpineConsumerDbContext db,
        ILogger<OrderProjectionService> logger)
    {
        _db = db;
        _logger = logger;
    }

    public async Task<bool> ApplyAsync(EventEnvelope envelope, CancellationToken ct = default)
    {
        await using var tx = await _db.Database.BeginTransactionAsync(ct);

        // 1. Idempotency guard
        var claimed = await _db.Database.ExecuteSqlInterpolatedAsync(
            $"""
             INSERT INTO consumer_inbox (event_id, processed_at)
             VALUES ({envelope.EventId}, NOW())
             ON CONFLICT (event_id) DO NOTHING
             """,
            ct);

        if (claimed == 0)
        {
            _logger.LogDebug("Event {EventId} already processed — replay skipped.", envelope.EventId);
            await tx.CommitAsync(ct);
            return false;
        }

        // 2. Dispatch by event type
        var applied = envelope.EventType switch
        {
            nameof(OrderCreated)   => await ApplyOrderCreated(envelope, ct),
            nameof(OrderPaid)      => await ApplyStatusChange(envelope, "Paid", ct),
            nameof(OrderShipped)   => await ApplyStatusChange(envelope, "Shipped", ct),
            nameof(OrderCancelled) => await ApplyStatusChange(envelope, "Cancelled", ct),
            _ => WarnUnknown(envelope.EventType),
        };

        await _db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        return applied;
    }

    private async Task<bool> ApplyOrderCreated(EventEnvelope envelope, CancellationToken ct)
    {
        var payload = JsonSerializer.Deserialize<OrderCreated>(envelope.Payload);
        if (payload is null)
        {
            _logger.LogWarning("OrderCreated {EventId} had null payload — skipping.", envelope.EventId);
            return false;
        }

        // Upsert semantics: same aggregate re-created (defensive — should not happen
        // in normal flow, but a duplicate OrderCreated for the same aggregate must
        // not throw a PK violation and stall the consumer).
        var existing = await _db.OrdersView.FindAsync(new object?[] { payload.OrderId }, ct);
        if (existing is not null)
        {
            _logger.LogWarning(
                "OrderCreated for existing aggregate {OrderId} — keeping existing view.",
                payload.OrderId);
            return false;
        }

        _db.OrdersView.Add(new OrderView
        {
            Id = payload.OrderId,
            Amount = payload.Amount,
            Status = "Created",
            CreatedAt = envelope.OccurredAt,
            UpdatedAt = envelope.OccurredAt,
            Version = 1,
        });
        return true;
    }

    private async Task<bool> ApplyStatusChange(EventEnvelope envelope, string newStatus, CancellationToken ct)
    {
        // Every status-changing event carries OrderId at minimum; deserialize as
        // the concrete type only for logs/audit later — for the projection, we
        // just need the aggregate id and the new status.
        var orderId = envelope.AggregateId;

        var view = await _db.OrdersView.FindAsync(new object?[] { orderId }, ct);
        if (view is null)
        {
            _logger.LogWarning(
                "Received {EventType} for unknown order {OrderId} — projection stays empty. " +
                "Likely out-of-order delivery across partitions or a data-loss upstream.",
                envelope.EventType, orderId);
            return false;
        }

        view.Status = newStatus;
        view.UpdatedAt = envelope.OccurredAt;
        view.Version += 1;
        return true;
    }

    private bool WarnUnknown(string eventType)
    {
        _logger.LogWarning("Unknown event type {EventType} — projection skipped.", eventType);
        return false;
    }
}

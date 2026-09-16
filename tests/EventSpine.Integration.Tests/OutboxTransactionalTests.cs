using EventSpine.Integration.Tests.Fixtures;
using EventSpine.Producer.Data;
using EventSpine.Producer.Domain;
using EventSpine.Producer.Services;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EventSpine.Integration.Tests;

/// <summary>
/// Prova que a transação do <see cref="OrderService"/> é atômica: cada mutação de
/// agregado deixa uma linha correspondente no outbox, ambas do mesmo commit.
/// </summary>
public sealed class OutboxTransactionalTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture _pg;
    private EventSpineDbContext _db = default!;
    private OrderService _svc = default!;

    public OutboxTransactionalTests(PostgresFixture pg) => _pg = pg;

    public Task InitializeAsync()
    {
        var opts = new DbContextOptionsBuilder<EventSpineDbContext>()
            .UseNpgsql(_pg.ConnectionString)
            .Options;
        _db = new EventSpineDbContext(opts);
        _svc = new OrderService(_db, TimeProvider.System);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        // Limpa tabelas entre testes desta classe.
        await _db.Database.ExecuteSqlRawAsync("TRUNCATE outbox, orders;");
        await _db.DisposeAsync();
    }

    [Fact]
    public async Task CreateOrder_writes_order_and_outbox_row_atomically()
    {
        var order = await _svc.CreateOrderAsync(amount: 199.90m);

        order.Status.Should().Be(OrderStatus.Created);

        // Order persistiu.
        var savedOrder = await _db.Orders.AsNoTracking().SingleAsync(o => o.Id == order.Id);
        savedOrder.Amount.Should().Be(199.90m);
        savedOrder.Status.Should().Be(OrderStatus.Created);

        // Outbox tem uma linha pro mesmo aggregate, com evento OrderCreated, sent_at null.
        var outboxRows = await _db.Outbox.AsNoTracking()
            .Where(m => m.AggregateId == order.Id)
            .ToListAsync();
        outboxRows.Should().ContainSingle();
        var msg = outboxRows.Single();
        msg.EventType.Should().Be("OrderCreated");
        msg.SentAt.Should().BeNull();
        msg.Payload.Should().Contain(order.Id.ToString());
        msg.Payload.Should().Contain("199.9");
    }

    [Fact]
    public async Task PayOrder_writes_second_outbox_row_atomically()
    {
        var order = await _svc.CreateOrderAsync(amount: 50m);

        await _svc.PayOrderAsync(order.Id);

        var updatedOrder = await _db.Orders.AsNoTracking().SingleAsync(o => o.Id == order.Id);
        updatedOrder.Status.Should().Be(OrderStatus.Paid);

        var outboxRows = await _db.Outbox.AsNoTracking()
            .Where(m => m.AggregateId == order.Id)
            .OrderBy(m => m.OccurredAt)
            .ToListAsync();
        outboxRows.Should().HaveCount(2);
        outboxRows[0].EventType.Should().Be("OrderCreated");
        outboxRows[1].EventType.Should().Be("OrderPaid");
    }

    [Fact]
    public async Task PayOrder_when_already_paid_throws_and_does_not_write_second_outbox()
    {
        var order = await _svc.CreateOrderAsync(amount: 100m);
        await _svc.PayOrderAsync(order.Id);

        var act = () => _svc.PayOrderAsync(order.Id);

        await act.Should().ThrowAsync<InvalidOperationException>();

        // Ainda só as duas linhas originais no outbox — nada de OrderPaid duplicado.
        var outboxCount = await _db.Outbox.CountAsync(m => m.AggregateId == order.Id);
        outboxCount.Should().Be(2);
    }
}

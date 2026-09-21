using Xunit;
using EventSpine.Consumer.Data;
using EventSpine.Consumer.Projection;
using EventSpine.Contracts.Events;
using EventSpine.Integration.Tests.Fixtures;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text.Json;

namespace EventSpine.Integration.Tests;

/// <summary>
/// Direct tests of <see cref="OrderProjectionService"/> against a real Postgres
/// (Testcontainers). No Kafka — the projection service is exercised by handing
/// it envelopes as they would be received from the consumer worker.
/// </summary>
public sealed class ConsumerProjectionTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture _pg;
    private EventSpineConsumerDbContext _db = null!;
    private OrderProjectionService _projection = null!;

    public ConsumerProjectionTests(PostgresFixture pg) => _pg = pg;

    public async Task InitializeAsync()
    {
        var options = new DbContextOptionsBuilder<EventSpineConsumerDbContext>()
            .UseNpgsql(_pg.ConnectionString)
            .Options;

        _db = new EventSpineConsumerDbContext(options);
        await ConsumerDbInitializer.InitializeAsync(_db);

        // Clean slate between tests — this fixture is shared across the class.
        await _db.Database.ExecuteSqlRawAsync("TRUNCATE TABLE consumer_inbox, orders_view");

        _projection = new OrderProjectionService(_db, NullLogger<OrderProjectionService>.Instance);
    }

    public async Task DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task Applies_OrderCreated_and_writes_view_with_version_1()
    {
        var orderId = Guid.NewGuid();
        var envelope = Envelope(nameof(OrderCreated), orderId,
            new OrderCreated(orderId, 100m, DateTime.UtcNow));

        var applied = await _projection.ApplyAsync(envelope);

        Assert.True(applied);

        var view = await _db.OrdersView.AsNoTracking().SingleAsync();
        Assert.Equal(orderId, view.Id);
        Assert.Equal(100m, view.Amount);
        Assert.Equal("Created", view.Status);
        Assert.Equal(1, view.Version);
    }

    [Fact]
    public async Task Applies_OrderPaid_after_created_and_bumps_version_to_2()
    {
        var orderId = Guid.NewGuid();
        var now = DateTime.UtcNow;

        await _projection.ApplyAsync(Envelope(nameof(OrderCreated), orderId,
            new OrderCreated(orderId, 50m, now)));

        var applied = await _projection.ApplyAsync(Envelope(nameof(OrderPaid), orderId,
            new OrderPaid(orderId, 50m, now.AddSeconds(1))));

        Assert.True(applied);

        var view = await _db.OrdersView.AsNoTracking().SingleAsync();
        Assert.Equal("Paid", view.Status);
        Assert.Equal(2, view.Version);
    }

    [Fact]
    public async Task Same_event_id_processed_twice_updates_view_only_once()
    {
        var orderId = Guid.NewGuid();
        var envelope = Envelope(nameof(OrderCreated), orderId,
            new OrderCreated(orderId, 200m, DateTime.UtcNow));

        var first = await _projection.ApplyAsync(envelope);
        var second = await _projection.ApplyAsync(envelope);

        Assert.True(first);
        Assert.False(second); // replay short-circuited by the inbox

        var view = await _db.OrdersView.AsNoTracking().SingleAsync();
        Assert.Equal(1, view.Version); // NOT 2

        var inboxCount = await _db.ConsumerInbox.CountAsync();
        Assert.Equal(1, inboxCount);
    }

    [Fact]
    public async Task OrderPaid_before_OrderCreated_does_not_create_view_and_does_not_throw()
    {
        var orphanOrderId = Guid.NewGuid();

        var applied = await _projection.ApplyAsync(Envelope(nameof(OrderPaid), orphanOrderId,
            new OrderPaid(orphanOrderId, 0m, DateTime.UtcNow)));

        Assert.False(applied);
        Assert.Empty(await _db.OrdersView.AsNoTracking().ToListAsync());

        // The inbox still records the event as processed — replaying it later
        // must not cause a second attempt at applying.
        Assert.Equal(1, await _db.ConsumerInbox.CountAsync());
    }

    private static EventEnvelope Envelope<T>(string type, Guid aggregateId, T payload)
    {
        var json = JsonSerializer.Serialize(payload);
        return new EventEnvelope(
            EventId: Guid.NewGuid(),
            EventType: type,
            AggregateId: aggregateId,
            OccurredAt: DateTime.UtcNow,
            Payload: json);
    }
}

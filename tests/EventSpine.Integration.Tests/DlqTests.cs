using Xunit;
using EventSpine.Consumer.Data;
using EventSpine.Consumer.Dlq;
using EventSpine.Consumer.Options;
using EventSpine.Contracts.Events;
using EventSpine.Integration.Tests.Fakes;
using EventSpine.Integration.Tests.Fixtures;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace EventSpine.Integration.Tests;

/// <summary>
/// Tests the DLQ handling path end-to-end at the service level:
/// dead-lettering (persists + publishes to fake Kafka), replay (marks +
/// republishes to main topic), idempotency (re-inserting same event_id
/// updates instead of duplicates), and the /dlq HTTP surface via the
/// service directly (endpoints just wrap it — covered by integration
/// smoke).
///
/// No real Kafka — <see cref="FakeKafkaTopicProducer"/> captures publishes
/// in memory. Postgres via <see cref="PostgresFixture"/>.
/// </summary>
public sealed class DlqTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture _pg;

    private EventSpineConsumerDbContext _db = null!;
    private FakeKafkaTopicProducer _kafka = null!;
    private DlqService _dlq = null!;
    private RetryOptions _retryOptions = null!;

    public DlqTests(PostgresFixture pg) => _pg = pg;

    public async Task InitializeAsync()
    {
        var options = new DbContextOptionsBuilder<EventSpineConsumerDbContext>()
            .UseNpgsql(_pg.ConnectionString)
            .Options;

        _db = new EventSpineConsumerDbContext(options);
        await ConsumerDbInitializer.InitializeAsync(_db);
        await _db.Database.ExecuteSqlRawAsync("TRUNCATE TABLE dlq_events, consumer_inbox, orders_view");

        _kafka = new FakeKafkaTopicProducer();
        _retryOptions = new RetryOptions
        {
            MaxAttempts = 3,
            BackoffBaseMs = 10,
            DlqTopicName = "orders.events.dlq",
            MainTopicName = "orders.events",
        };

        _dlq = new DlqService(
            _db,
            _kafka,
            Options.Create(_retryOptions),
            NullLogger<DlqService>.Instance);
    }

    public async Task DisposeAsync() => await _db.DisposeAsync();

    [Fact]
    public async Task HandleDeadLetter_persists_row_and_publishes_to_DLQ_topic()
    {
        var envelope = MakeEnvelope();
        var raw = JsonSerializer.Serialize(envelope);

        await _dlq.HandleDeadLetterAsync(envelope, raw,
            reason: "projection_failure",
            errorMessage: "boom",
            attempts: 3);

        var row = await _db.DlqEvents.AsNoTracking().SingleAsync();
        Assert.Equal(envelope.EventId, row.EventId);
        Assert.Equal("projection_failure", row.Reason);
        Assert.Equal("boom", row.ErrorMessage);
        Assert.Equal(3, row.Attempts);
        Assert.Null(row.ReplayedAt);

        var sent = _kafka.To("orders.events.dlq");
        Assert.Single(sent);
        Assert.Equal(envelope.AggregateId.ToString(), sent[0].Key);
    }

    [Fact]
    public async Task HandleDeadLetter_is_idempotent_on_repeated_event_id()
    {
        var envelope = MakeEnvelope();
        var raw = JsonSerializer.Serialize(envelope);

        await _dlq.HandleDeadLetterAsync(envelope, raw, "projection_failure", "first error", 3);
        await _dlq.HandleDeadLetterAsync(envelope, raw, "projection_failure", "second error", 3);

        var rows = await _db.DlqEvents.AsNoTracking().ToListAsync();
        Assert.Single(rows);
        Assert.Equal("second error", rows[0].ErrorMessage);
    }

    [Fact]
    public async Task HandleDeadLetter_survives_Kafka_failure_and_still_persists_row()
    {
        _kafka.ThrowOnProduce = new InvalidOperationException("broker down");

        var envelope = MakeEnvelope();
        var raw = JsonSerializer.Serialize(envelope);

        await _dlq.HandleDeadLetterAsync(envelope, raw, "projection_failure", "boom", 3);

        // Table is the source of truth — it must have the entry even when the
        // DLQ topic publish fails.
        var row = await _db.DlqEvents.AsNoTracking().SingleAsync();
        Assert.Equal(envelope.EventId, row.EventId);
        Assert.Empty(_kafka.To("orders.events.dlq")); // publish failed, nothing captured
    }

    [Fact]
    public async Task Replay_publishes_to_main_topic_and_marks_replayed()
    {
        // Arrange: insert a dlq_events row manually
        var envelope = MakeEnvelope();
        var raw = JsonSerializer.Serialize(envelope);
        await _dlq.HandleDeadLetterAsync(envelope, raw, "projection_failure", "boom", 3);
        _kafka.Sends.Clear(); // ignore the DLQ-topic publish, focus on replay

        var entry = await _db.DlqEvents.AsNoTracking().SingleAsync();

        // Act
        var replayed = await _dlq.ReplayAsync(entry.Id);

        // Assert: row updated
        Assert.NotNull(replayed);
        Assert.NotNull(replayed!.ReplayedAt);
        Assert.Equal(1, replayed.ReplayedCount);

        // Assert: main topic received the raw envelope back
        var sent = _kafka.To("orders.events");
        Assert.Single(sent);
        Assert.Equal(envelope.AggregateId.ToString(), sent[0].Key);
        Assert.Equal(raw, sent[0].Value);
    }

    [Fact]
    public async Task Replay_returns_null_for_unknown_id()
    {
        var result = await _dlq.ReplayAsync(Guid.NewGuid());
        Assert.Null(result);
    }

    private static EventEnvelope MakeEnvelope()
    {
        var orderId = Guid.NewGuid();
        var payload = new OrderCreated(orderId, 100m, DateTime.UtcNow);
        return new EventEnvelope(
            EventId: Guid.NewGuid(),
            EventType: nameof(OrderCreated),
            AggregateId: orderId,
            OccurredAt: DateTime.UtcNow,
            Payload: JsonSerializer.Serialize(payload));
    }
}

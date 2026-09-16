using System.Text.Json;
using Confluent.Kafka;
using EventSpine.Contracts.Events;
using EventSpine.Integration.Tests.Fixtures;
using EventSpine.Producer.Data;
using EventSpine.Producer.Outbox;
using EventSpine.Producer.Services;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventSpine.Integration.Tests;

/// <summary>
/// Fluxo end-to-end: <see cref="OrderService"/> grava order + outbox atomicamente,
/// <see cref="OutboxRelay"/> publica no Kafka, marca <c>SentAt</c>. Verificamos
/// que a mensagem chega no tópico com key correta (AggregateId) e envelope válido.
/// </summary>
public sealed class OutboxRelayTests : IClassFixture<PostgresFixture>, IClassFixture<RedpandaFixture>, IAsyncLifetime
{
    private readonly PostgresFixture _pg;
    private readonly RedpandaFixture _redpanda;
    private readonly string _topicName = $"orders.events.test.{Guid.NewGuid():N}";

    private ServiceProvider _provider = default!;
    private EventSpineDbContext _db = default!;
    private OrderService _svc = default!;
    private OutboxRelay _relay = default!;

    public OutboxRelayTests(PostgresFixture pg, RedpandaFixture redpanda)
    {
        _pg = pg;
        _redpanda = redpanda;
    }

    public Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.Configure<KafkaOptions>(o =>
        {
            o.BootstrapServers = _redpanda.BootstrapServers;
            o.ClientId = "event-spine-producer-tests";
        });
        services.Configure<OutboxOptions>(o =>
        {
            o.TopicName = _topicName;
            o.BatchSize = 100;
            o.PollInterval = TimeSpan.FromMilliseconds(50);
        });
        services.AddDbContext<EventSpineDbContext>(opt => opt.UseNpgsql(_pg.ConnectionString));
        services.AddScoped<OrderService>();
        services.AddSingleton<KafkaEventPublisher>();

        _provider = services.BuildServiceProvider();

        // Contexto próprio pra asserções (não compartilha com o do relay).
        var opts = new DbContextOptionsBuilder<EventSpineDbContext>()
            .UseNpgsql(_pg.ConnectionString)
            .Options;
        _db = new EventSpineDbContext(opts);
        _svc = new OrderService(_db, TimeProvider.System);

        _relay = new OutboxRelay(
            _provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<KafkaEventPublisher>(),
            _provider.GetRequiredService<IOptions<OutboxOptions>>(),
            TimeProvider.System,
            NullLogger<OutboxRelay>.Instance);

        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _db.Database.ExecuteSqlRawAsync("TRUNCATE outbox, orders;");
        await _db.DisposeAsync();
        await _provider.DisposeAsync();
    }

    [Fact]
    public async Task Relay_publishes_pending_messages_and_marks_them_sent()
    {
        var order = await _svc.CreateOrderAsync(amount: 250m);

        // Executa um batch do relay diretamente (sem BackgroundService loop).
        var processed = await _relay.ProcessBatchAsync(CancellationToken.None);

        processed.Should().Be(1);

        // Message chegou no tópico com key correta.
        var envelope = ReadOneEnvelope(_topicName, _redpanda.BootstrapServers, TimeSpan.FromSeconds(10));
        envelope.EventType.Should().Be("OrderCreated");
        envelope.AggregateId.Should().Be(order.Id);
        envelope.Payload.Should().Contain(order.Id.ToString());

        // Outbox row foi marcada como enviada.
        var msg = await _db.Outbox.AsNoTracking().SingleAsync(m => m.AggregateId == order.Id);
        msg.SentAt.Should().NotBeNull();
    }

    [Fact]
    public async Task Relay_keeps_order_by_key_across_two_events_from_same_aggregate()
    {
        var order = await _svc.CreateOrderAsync(amount: 300m);
        await _svc.PayOrderAsync(order.Id);

        await _relay.ProcessBatchAsync(CancellationToken.None);

        var envelopes = ReadEnvelopes(_topicName, _redpanda.BootstrapServers, expected: 2, TimeSpan.FromSeconds(10));
        envelopes.Should().HaveCount(2);
        envelopes[0].EventType.Should().Be("OrderCreated");
        envelopes[1].EventType.Should().Be("OrderPaid");
        envelopes.Should().OnlyContain(e => e.AggregateId == order.Id);
    }

    private static EventEnvelope ReadOneEnvelope(string topic, string bootstrap, TimeSpan timeout) =>
        ReadEnvelopes(topic, bootstrap, expected: 1, timeout).Single();

    private static List<EventEnvelope> ReadEnvelopes(string topic, string bootstrap, int expected, TimeSpan timeout)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"test-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var result = new List<EventEnvelope>();
        var deadline = DateTime.UtcNow + timeout;
        while (result.Count < expected && DateTime.UtcNow < deadline)
        {
            var msg = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (msg?.Message is null)
            {
                continue;
            }

            var envelope = JsonSerializer.Deserialize<EventEnvelope>(msg.Message.Value, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            });
            if (envelope is not null)
            {
                result.Add(envelope);
            }
        }

        consumer.Close();
        return result;
    }
}

using Confluent.Kafka;
using EventSpine.Consumer.Options;
using EventSpine.Consumer.Projection;
using EventSpine.Contracts.Events;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace EventSpine.Consumer.Kafka;

/// <summary>
/// Long-running worker that subscribes to the orders.events topic and applies
/// each envelope to the projection through <see cref="OrderProjectionService"/>.
///
/// Commit strategy: EnableAutoCommit=false + manual StoreOffset+Commit on every
/// message. Combined with the inbox-based idempotency downstream, this gives
/// "effectively-once" semantics for the projection even if the process is
/// killed between processing and commit.
/// </summary>
public sealed class OrderEventsConsumer : BackgroundService
{
    private readonly KafkaConsumerOptions _opts;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OrderEventsConsumer> _logger;

    public OrderEventsConsumer(
        IOptions<KafkaConsumerOptions> opts,
        IServiceScopeFactory scopeFactory,
        ILogger<OrderEventsConsumer> logger)
    {
        _opts = opts.Value;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _opts.BootstrapServers,
            GroupId = _opts.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_opts.TopicName);
        _logger.LogInformation("Consumer subscribed to {Topic} as group {Group}",
            _opts.TopicName, _opts.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessOnceAsync(consumer, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// Consumes at most one message and applies it. Internal so integration tests
    /// can drive the loop deterministically without racing the BackgroundService.
    /// </summary>
    internal async Task ProcessOnceAsync(IConsumer<string, string> consumer, CancellationToken ct)
    {
        ConsumeResult<string, string>? result;
        try
        {
            result = consumer.Consume(TimeSpan.FromSeconds(1));
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Consume failed — will retry on next poll.");
            return;
        }

        if (result is null || result.Message is null) return;

        EventEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope>(result.Message.Value);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex,
                "Poison message at offset {Offset} — malformed JSON. Committing to skip; " +
                "DLQ path lands in Bloco 4.",
                result.Offset);
            consumer.StoreOffset(result);
            consumer.Commit(result);
            return;
        }

        if (envelope is null)
        {
            _logger.LogWarning("Envelope deserialized to null at offset {Offset} — skipping.",
                result.Offset);
            consumer.StoreOffset(result);
            consumer.Commit(result);
            return;
        }

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var projection = scope.ServiceProvider.GetRequiredService<OrderProjectionService>();
            await projection.ApplyAsync(envelope, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Projection failed for event {EventId} at offset {Offset} — offset NOT committed, " +
                "message will be re-delivered. Poison-message handling lands in Bloco 4.",
                envelope.EventId, result.Offset);
            return; // do not commit — will retry
        }

        consumer.StoreOffset(result);
        consumer.Commit(result);
    }
}

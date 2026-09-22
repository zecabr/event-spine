using Confluent.Kafka;
using EventSpine.Consumer.Dlq;
using EventSpine.Consumer.Options;
using EventSpine.Consumer.Projection;
using EventSpine.Contracts.Events;
using EventSpine.Contracts.Validation;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace EventSpine.Consumer.Kafka;

/// <summary>
/// Long-running worker that subscribes to the orders.events topic and applies
/// each envelope to the projection through <see cref="OrderProjectionService"/>.
///
/// Failure policy:
///   - JSON deserialization fails → "poison": dead-letter immediately + commit.
///   - Schema validation fails → "schema_violation": dead-letter + commit (no retry —
///     the payload will not become valid by retrying).
///   - Projection throws → retry inline up to RetryOptions.MaxAttempts with
///     exponential backoff; on the final failure, dead-letter with
///     reason="projection_failure" + commit.
///   - Success: commit and move to the next message.
///
/// Combined with the inbox-based idempotency inside the projection, this gives
/// "effectively-once" delivery: replays land on the inbox short-circuit, DLQd
/// events sit in dlq_events for inspection and replay via POST /dlq/{id}/replay.
/// </summary>
public sealed class OrderEventsConsumer : BackgroundService
{
    private readonly KafkaConsumerOptions _kafkaOptions;
    private readonly RetryOptions _retryOptions;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly SchemaValidator _schemaValidator;
    private readonly ILogger<OrderEventsConsumer> _logger;

    public OrderEventsConsumer(
        IOptions<KafkaConsumerOptions> kafkaOptions,
        IOptions<RetryOptions> retryOptions,
        IServiceScopeFactory scopeFactory,
        SchemaValidator schemaValidator,
        ILogger<OrderEventsConsumer> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _retryOptions = retryOptions.Value;
        _scopeFactory = scopeFactory;
        _schemaValidator = schemaValidator;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_kafkaOptions.TopicName);
        _logger.LogInformation("Consumer subscribed to {Topic} as group {Group}",
            _kafkaOptions.TopicName, _kafkaOptions.GroupId);

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

        if (result?.Message is null) return;

        var rawPayload = result.Message.Value;

        // 1. Poison-message path: malformed JSON goes straight to the DLQ.
        EventEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope>(rawPayload);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex,
                "Poison message at offset {Offset} — malformed JSON, sending to DLQ.",
                result.Offset);
            await DeadLetterAsync(envelope: null, rawPayload, reason: "poison",
                errorMessage: ex.Message, attempts: 0, ct);
            CommitSafely(consumer, result);
            return;
        }

        if (envelope is null)
        {
            _logger.LogWarning("Envelope null at offset {Offset} — skipping.", result.Offset);
            CommitSafely(consumer, result);
            return;
        }

        // 2. Schema validation — no retry, schema violations are permanent.
        var schemaResult = _schemaValidator.Validate(envelope.EventType, envelope.Payload);
        if (!schemaResult.IsValid)
        {
            _logger.LogError(
                "Schema violation for event {EventId} of type {EventType}: {Errors}",
                envelope.EventId, envelope.EventType, schemaResult.ErrorSummary);
            await DeadLetterAsync(envelope, rawPayload, reason: "schema_violation",
                errorMessage: schemaResult.ErrorSummary, attempts: 0, ct);
            CommitSafely(consumer, result);
            return;
        }

        // 3. Retry loop with exponential backoff.
        Exception? lastError = null;
        for (var attempt = 1; attempt <= _retryOptions.MaxAttempts; attempt++)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var projection = scope.ServiceProvider.GetRequiredService<OrderProjectionService>();
                await projection.ApplyAsync(envelope, ct);

                CommitSafely(consumer, result);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lastError = ex;
                _logger.LogWarning(ex,
                    "Attempt {Attempt}/{Max} failed for event {EventId}.",
                    attempt, _retryOptions.MaxAttempts, envelope.EventId);

                if (attempt < _retryOptions.MaxAttempts)
                {
                    var delayMs = _retryOptions.BackoffBaseMs * (int)Math.Pow(2, attempt - 1);
                    await Task.Delay(delayMs, ct);
                }
            }
        }

        // 4. Retries exhausted → DLQ + commit.
        _logger.LogError(lastError,
            "Event {EventId} failed all {Max} attempts — sending to DLQ.",
            envelope.EventId, _retryOptions.MaxAttempts);

        await DeadLetterAsync(envelope, rawPayload, reason: "projection_failure",
            errorMessage: lastError?.Message ?? "unknown",
            attempts: _retryOptions.MaxAttempts, ct);

        CommitSafely(consumer, result);
    }

    private async Task DeadLetterAsync(
        EventEnvelope? envelope,
        string rawPayload,
        string reason,
        string errorMessage,
        int attempts,
        CancellationToken ct)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var dlq = scope.ServiceProvider.GetRequiredService<DlqService>();
            await dlq.HandleDeadLetterAsync(envelope, rawPayload, reason, errorMessage, attempts, ct);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex,
                "DLQ write FAILED for reason={Reason}. Offset will still be committed to unblock the partition. " +
                "Manual recovery required.",
                reason);
        }
    }

    private static void CommitSafely(IConsumer<string, string> consumer, ConsumeResult<string, string> result)
    {
        consumer.StoreOffset(result);
        consumer.Commit(result);
    }
}

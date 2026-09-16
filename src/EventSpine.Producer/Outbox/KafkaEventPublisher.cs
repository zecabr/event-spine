using System.Text.Json;
using Confluent.Kafka;
using EventSpine.Contracts.Events;
using EventSpine.Producer.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventSpine.Producer.Outbox;

/// <summary>
/// Wrapper fino sobre <see cref="IProducer{TKey, TValue}"/> do Confluent.Kafka.
/// Serializa a <see cref="OutboxMessage"/> em <see cref="EventEnvelope"/>, publica
/// no tópico configurado usando <c>AggregateId</c> como key (garante ordem por
/// aggregate — ver ADR-001) e devolve o resultado do produce.
/// </summary>
public sealed class KafkaEventPublisher : IAsyncDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly IProducer<string, string> _producer;
    private readonly OutboxOptions _outboxOptions;
    private readonly ILogger<KafkaEventPublisher> _logger;

    public KafkaEventPublisher(
        IOptions<KafkaOptions> kafkaOptions,
        IOptions<OutboxOptions> outboxOptions,
        ILogger<KafkaEventPublisher> logger)
    {
        var kafka = kafkaOptions.Value;
        _outboxOptions = outboxOptions.Value;
        _logger = logger;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            ClientId = kafka.ClientId,
            EnableIdempotence = true, // dedup no broker por retries
            Acks = Acks.All,          // espera ack de todas as réplicas ISR
            MessageTimeoutMs = 10_000,
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        var envelope = new EventEnvelope(
            EventId: message.Id,
            EventType: message.EventType,
            AggregateId: message.AggregateId,
            OccurredAt: message.OccurredAt,
            Payload: message.Payload);

        var kafkaMessage = new Message<string, string>
        {
            Key = message.AggregateId.ToString(),
            Value = JsonSerializer.Serialize(envelope, JsonOptions),
        };

        var deliveryResult = await _producer.ProduceAsync(_outboxOptions.TopicName, kafkaMessage, ct)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "outbox message {MessageId} publicada em {Topic}@{Partition}:{Offset}",
            message.Id, deliveryResult.Topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}

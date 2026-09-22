using Confluent.Kafka;
using EventSpine.Consumer.Options;
using Microsoft.Extensions.Options;

namespace EventSpine.Consumer.Dlq;

/// <summary>
/// Real Confluent.Kafka-backed producer used at runtime.
///
/// Kept intentionally simple: idempotent, acks=all, one producer for the whole
/// process. Consumer service publishes at low rates (only DLQ + replays), so a
/// single producer is more than enough.
/// </summary>
public sealed class KafkaTopicProducer : IKafkaTopicProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaTopicProducer> _logger;

    public KafkaTopicProducer(
        IOptions<KafkaConsumerOptions> kafkaOptions,
        ILogger<KafkaTopicProducer> logger)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            ClientId = $"event-spine-consumer-{Environment.MachineName}",
        };
        _producer = new ProducerBuilder<string, string>(config).Build();
        _logger = logger;
    }

    public async Task ProduceAsync(string topic, string key, string value, CancellationToken ct = default)
    {
        var result = await _producer.ProduceAsync(topic,
            new Message<string, string> { Key = key, Value = value },
            ct);

        _logger.LogDebug("Produced to {Topic} partition {Partition} offset {Offset}",
            topic, result.Partition, result.Offset);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}

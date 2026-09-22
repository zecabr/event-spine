namespace EventSpine.Consumer.Dlq;

/// <summary>
/// Thin abstraction over the Kafka producer used by the Consumer service.
/// Extracted as an interface so integration tests can substitute a fake
/// in-memory implementation without booting a real broker.
///
/// The abstraction is intentionally minimal: this project publishes JSON
/// envelopes keyed by aggregate id to exactly two topics (main + DLQ), and
/// nothing else. Widening the surface earns nothing.
/// </summary>
public interface IKafkaTopicProducer
{
    Task ProduceAsync(string topic, string key, string value, CancellationToken ct = default);
}

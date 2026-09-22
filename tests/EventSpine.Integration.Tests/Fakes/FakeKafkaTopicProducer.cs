using EventSpine.Consumer.Dlq;
using System.Collections.Concurrent;

namespace EventSpine.Integration.Tests.Fakes;

/// <summary>
/// In-memory stand-in for <see cref="IKafkaTopicProducer"/>. Captures every
/// (topic, key, value) triple sent by the code under test so integration tests
/// can assert on what would have been published without booting a real broker.
///
/// Concurrent-safe (ConcurrentQueue) because the consumer runs projections in
/// scoped services that may cross thread boundaries.
/// </summary>
public sealed class FakeKafkaTopicProducer : IKafkaTopicProducer
{
    public sealed record Sent(string Topic, string Key, string Value);

    public ConcurrentQueue<Sent> Sends { get; } = new();

    /// <summary>If set, every ProduceAsync call throws this — for testing failure paths.</summary>
    public Exception? ThrowOnProduce { get; set; }

    public Task ProduceAsync(string topic, string key, string value, CancellationToken ct = default)
    {
        if (ThrowOnProduce is not null) throw ThrowOnProduce;
        Sends.Enqueue(new Sent(topic, key, value));
        return Task.CompletedTask;
    }

    public IReadOnlyList<Sent> To(string topic)
        => Sends.Where(s => s.Topic == topic).ToList();
}

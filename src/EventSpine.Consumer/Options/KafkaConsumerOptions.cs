namespace EventSpine.Consumer.Options;

public sealed class KafkaConsumerOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "event-spine.consumer";
    public string TopicName { get; set; } = "orders.events";
}

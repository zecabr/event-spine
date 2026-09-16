namespace EventSpine.Producer.Outbox;

/// <summary>Config do produtor Kafka — lê seção <c>Kafka</c>.</summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    /// <summary>Bootstrap servers no formato <c>host:port[,host2:port2,...]</c>. Ex.: <c>localhost:9092</c>.</summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Client id que aparece nos logs do broker. Default: <c>event-spine-producer</c>.</summary>
    public string ClientId { get; set; } = "event-spine-producer";
}

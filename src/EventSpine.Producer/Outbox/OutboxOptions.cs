namespace EventSpine.Producer.Outbox;

/// <summary>Config do <c>OutboxRelay</c> — lê seção <c>Outbox</c> do appsettings/env.</summary>
public sealed class OutboxOptions
{
    public const string SectionName = "Outbox";

    /// <summary>Nome do tópico Kafka onde os eventos vão. Default: <c>orders.events</c>.</summary>
    public string TopicName { get; set; } = "orders.events";

    /// <summary>Intervalo entre polls do outbox quando não há backlog. Default: 200 ms.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Máximo de mensagens lidas por poll. Default: 100.</summary>
    public int BatchSize { get; set; } = 100;
}

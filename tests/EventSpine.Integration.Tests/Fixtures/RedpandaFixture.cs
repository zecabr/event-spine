using Testcontainers.Redpanda;
using Xunit;

namespace EventSpine.Integration.Tests.Fixtures;

/// <summary>
/// Sobe um Redpanda (Kafka-compat) via Testcontainers. Broker efêmero por
/// classe — evita interferência entre testes.
/// </summary>
public sealed class RedpandaFixture : IAsyncLifetime
{
    private readonly RedpandaContainer _container = new RedpandaBuilder()
        .WithImage("docker.redpanda.com/redpandadata/redpanda:v24.3.1")
        .Build();

    public string BootstrapServers => _container.GetBootstrapAddress();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

using EventSpine.Producer.Data;
using Testcontainers.PostgreSql;
using Xunit;

namespace EventSpine.Integration.Tests.Fixtures;

/// <summary>
/// Sobe um Postgres 16 via Testcontainers, uma vez por classe de teste.
/// <see cref="ConnectionString"/> aponta pro container efêmero — cada instância
/// da fixture é isolada, então testes rodam em paralelo entre classes sem colisão.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("event_spine")
        .WithUsername("event_spine")
        .WithPassword("event_spine")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync().ConfigureAwait(false);
        await DbInitializer.EnsureCreatedAsync(ConnectionString).ConfigureAwait(false);
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

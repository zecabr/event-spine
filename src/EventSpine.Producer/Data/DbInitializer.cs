using Npgsql;

namespace EventSpine.Producer.Data;

/// <summary>
/// Cria as tabelas do Producer (orders + outbox) se ainda não existirem, via SQL raw.
/// Rodado uma vez na startup — idempotente. Migrations formais entram quando o
/// schema evoluir de forma incompatível.
/// </summary>
public static class DbInitializer
{
    private const string Schema = """
        CREATE TABLE IF NOT EXISTS orders (
            id          UUID           PRIMARY KEY,
            amount      NUMERIC(18, 2) NOT NULL,
            status      VARCHAR(32)    NOT NULL,
            created_at  TIMESTAMPTZ    NOT NULL,
            updated_at  TIMESTAMPTZ    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id            UUID         PRIMARY KEY,
            event_type    VARCHAR(64)  NOT NULL,
            aggregate_id  UUID         NOT NULL,
            occurred_at   TIMESTAMPTZ  NOT NULL,
            payload       JSONB        NOT NULL,
            sent_at       TIMESTAMPTZ  NULL
        );

        -- Index parcial: só linhas pendentes (SentAt IS NULL) entram, mantém index
        -- pequeno mesmo com histórico grande de eventos já enviados.
        CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox (id) WHERE sent_at IS NULL;
        """;

    public static async Task EnsureCreatedAsync(string connectionString, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(Schema, conn);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

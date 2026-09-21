using Microsoft.EntityFrameworkCore;

namespace EventSpine.Consumer.Data;

/// <summary>
/// Idempotent bootstrap of the consumer schema — safe to run on every start.
/// Bloco v0.1 uses raw SQL rather than EF migrations: keeps the schema visible
/// in one place and skirts the migration-vs-startup ordering that would trip
/// integration tests with Testcontainers.
/// </summary>
public static class ConsumerDbInitializer
{
    public static async Task InitializeAsync(EventSpineConsumerDbContext db, CancellationToken ct = default)
    {
        await db.Database.ExecuteSqlRawAsync(
            """
            CREATE TABLE IF NOT EXISTS consumer_inbox (
                event_id     UUID PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS orders_view (
                id         UUID PRIMARY KEY,
                amount     NUMERIC(18,2) NOT NULL,
                status     VARCHAR(32)   NOT NULL,
                created_at TIMESTAMP     NOT NULL,
                updated_at TIMESTAMP     NOT NULL,
                version    BIGINT        NOT NULL DEFAULT 0
            );
            """,
            ct);
    }
}

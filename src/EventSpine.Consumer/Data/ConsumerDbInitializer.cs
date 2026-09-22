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

            CREATE TABLE IF NOT EXISTS dlq_events (
                id              UUID PRIMARY KEY,
                event_id        UUID NOT NULL,
                event_type      VARCHAR(64),
                aggregate_id    UUID,
                payload         TEXT NOT NULL,
                reason          VARCHAR(64) NOT NULL,
                error_message   TEXT,
                attempts        INT NOT NULL,
                first_failed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                last_failed_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                replayed_at     TIMESTAMP,
                replayed_count  INT NOT NULL DEFAULT 0
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ix_dlq_events_event_id ON dlq_events(event_id);
            CREATE INDEX IF NOT EXISTS ix_dlq_events_reason ON dlq_events(reason) WHERE replayed_at IS NULL;
            """,
            ct);
    }
}

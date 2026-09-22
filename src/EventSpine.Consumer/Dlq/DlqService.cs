using EventSpine.Consumer.Data;
using EventSpine.Consumer.Options;
using EventSpine.Contracts.Events;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace EventSpine.Consumer.Dlq;

/// <summary>
/// Orchestrates the DLQ side of the pipeline.
///
/// - <see cref="HandleDeadLetterAsync"/> persists a failed event to
///   dlq_events (source of truth) and best-effort publishes to the DLQ
///   Kafka topic. Called by the consumer after the retry budget is
///   exhausted, and by the poison-message path when the envelope cannot
///   be deserialized.
/// - <see cref="ReplayAsync"/> reads a dlq_events row, republishes the
///   raw envelope to the main topic, and marks replayed_at / replayed_count.
///   Returns null if the id is not found.
///
/// The service is DbContext-scoped and safe to resolve per-request /
/// per-message.
/// </summary>
public sealed class DlqService
{
    private readonly EventSpineConsumerDbContext _db;
    private readonly IKafkaTopicProducer _producer;
    private readonly RetryOptions _retryOptions;
    private readonly ILogger<DlqService> _logger;

    public DlqService(
        EventSpineConsumerDbContext db,
        IKafkaTopicProducer producer,
        IOptions<RetryOptions> retryOptions,
        ILogger<DlqService> logger)
    {
        _db = db;
        _producer = producer;
        _retryOptions = retryOptions.Value;
        _logger = logger;
    }

    /// <summary>
    /// Records a failed event in dlq_events and (best-effort) publishes it to
    /// the DLQ topic. Idempotent by event_id: a second call for the same event
    /// updates attempts and last_failed_at instead of inserting a duplicate.
    /// </summary>
    public async Task HandleDeadLetterAsync(
        EventEnvelope? envelope,
        string rawPayload,
        string reason,
        string errorMessage,
        int attempts,
        CancellationToken ct = default)
    {
        var eventId = envelope?.EventId ?? Guid.NewGuid();

        var existing = await _db.DlqEvents
            .FirstOrDefaultAsync(e => e.EventId == eventId, ct);

        if (existing is null)
        {
            _db.DlqEvents.Add(new DlqEvent
            {
                Id = Guid.NewGuid(),
                EventId = eventId,
                EventType = envelope?.EventType,
                AggregateId = envelope?.AggregateId,
                Payload = rawPayload,
                Reason = reason,
                ErrorMessage = Truncate(errorMessage, 4000),
                Attempts = attempts,
                FirstFailedAt = DateTime.UtcNow,
                LastFailedAt = DateTime.UtcNow,
            });
        }
        else
        {
            existing.Attempts = attempts;
            existing.LastFailedAt = DateTime.UtcNow;
            existing.ErrorMessage = Truncate(errorMessage, 4000);
            existing.Reason = reason;
        }

        await _db.SaveChangesAsync(ct);

        // Best-effort DLQ topic publication. If the broker is unreachable, the
        // table entry is already committed — we just log and move on.
        try
        {
            var key = envelope?.AggregateId.ToString() ?? eventId.ToString();
            await _producer.ProduceAsync(_retryOptions.DlqTopicName, key, rawPayload, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to publish event {EventId} to DLQ topic {Topic} — dlq_events row already persisted.",
                eventId, _retryOptions.DlqTopicName);
        }
    }

    /// <summary>
    /// Republishes a dead-lettered event to the main topic and marks the row
    /// as replayed. Returns the updated row, or null if not found.
    /// </summary>
    public async Task<DlqEvent?> ReplayAsync(Guid dlqEventId, CancellationToken ct = default)
    {
        var entry = await _db.DlqEvents.FirstOrDefaultAsync(e => e.Id == dlqEventId, ct);
        if (entry is null) return null;

        // Extract key from the original envelope so ordering-by-aggregate holds
        // in the replay too.
        string key;
        try
        {
            var envelope = JsonSerializer.Deserialize<EventEnvelope>(entry.Payload);
            key = envelope?.AggregateId.ToString() ?? entry.EventId.ToString();
        }
        catch
        {
            // Poison payload — the replay is unlikely to help downstream, but we
            // still publish so an operator can see what re-arrived. Key by event_id.
            key = entry.EventId.ToString();
        }

        await _producer.ProduceAsync(_retryOptions.MainTopicName, key, entry.Payload, ct);

        entry.ReplayedAt = DateTime.UtcNow;
        entry.ReplayedCount += 1;
        await _db.SaveChangesAsync(ct);

        _logger.LogInformation(
            "Replayed DLQ entry {DlqId} (event {EventId}) to topic {Topic}. Replay count now {Count}.",
            entry.Id, entry.EventId, _retryOptions.MainTopicName, entry.ReplayedCount);

        return entry;
    }

    private static string? Truncate(string? value, int maxLen)
        => value is null || value.Length <= maxLen ? value : value[..maxLen];
}

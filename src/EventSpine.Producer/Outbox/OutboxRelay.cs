using EventSpine.Producer.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventSpine.Producer.Outbox;

/// <summary>
/// <c>BackgroundService</c> que faz poll periódico da tabela <c>outbox</c> por
/// mensagens não enviadas (<c>SentAt IS NULL</c>), publica cada uma no Kafka via
/// <see cref="KafkaEventPublisher"/> e marca <c>SentAt = now()</c> após o ack.
///
/// Reentrante: se crash entre publish e mark sent, o próximo poll re-publica —
/// consumer descarta via idempotência por <c>EventId</c> (Bloco 3).
/// </summary>
public sealed class OutboxRelay : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly KafkaEventPublisher _publisher;
    private readonly OutboxOptions _options;
    private readonly TimeProvider _clock;
    private readonly ILogger<OutboxRelay> _logger;

    public OutboxRelay(
        IServiceScopeFactory scopeFactory,
        KafkaEventPublisher publisher,
        IOptions<OutboxOptions> options,
        TimeProvider clock,
        ILogger<OutboxRelay> logger)
    {
        _scopeFactory = scopeFactory;
        _publisher = publisher;
        _options = options.Value;
        _clock = clock;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "OutboxRelay iniciado. Poll interval: {Interval} ms, batch size: {Batch}, topic: {Topic}",
            _options.PollInterval.TotalMilliseconds, _options.BatchSize, _options.TopicName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await ProcessBatchAsync(stoppingToken).ConfigureAwait(false);

                // Se o batch veio cheio, provavelmente há backlog — não espera.
                if (processed < _options.BatchSize)
                {
                    await Task.Delay(_options.PollInterval, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "OutboxRelay: erro no ciclo de poll. Vai retentar após {Backoff}", _options.PollInterval);
                await Task.Delay(_options.PollInterval, stoppingToken).ConfigureAwait(false);
            }
        }

        _logger.LogInformation("OutboxRelay encerrado.");
    }

    /// <summary>Processa um batch. Retorna quantas mensagens foram publicadas.</summary>
    internal async Task<int> ProcessBatchAsync(CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<EventSpineDbContext>();

        var pending = await db.Outbox
            .Where(m => m.SentAt == null)
            .OrderBy(m => m.OccurredAt)
            .Take(_options.BatchSize)
            .ToListAsync(ct)
            .ConfigureAwait(false);

        if (pending.Count == 0)
        {
            return 0;
        }

        foreach (var message in pending)
        {
            await _publisher.PublishAsync(message, ct).ConfigureAwait(false);
            message.SentAt = _clock.GetUtcNow().UtcDateTime;
        }

        await db.SaveChangesAsync(ct).ConfigureAwait(false);
        return pending.Count;
    }
}

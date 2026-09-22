using EventSpine.Consumer.Data;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.EntityFrameworkCore;

namespace EventSpine.Consumer.Dlq;

public static class DlqEndpoints
{
    public static IEndpointRouteBuilder MapDlqEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/dlq").WithTags("dlq");

        group.MapGet("/", List);
        group.MapGet("/summary", Summary);
        group.MapGet("/{id:guid}", GetById);
        group.MapPost("/{id:guid}/replay", Replay);

        return app;
    }

    /// <summary>
    /// Lists dead-lettered events. Defaults to non-replayed entries; pass
    /// ?includeReplayed=true to include those that were already replayed.
    /// </summary>
    private static async Task<Ok<IReadOnlyList<DlqEvent>>> List(
        EventSpineConsumerDbContext db,
        CancellationToken ct,
        bool includeReplayed = false)
    {
        var query = db.DlqEvents.AsNoTracking();
        if (!includeReplayed)
            query = query.Where(e => e.ReplayedAt == null);

        var rows = await query
            .OrderByDescending(e => e.LastFailedAt)
            .Take(200)
            .ToListAsync(ct);

        return TypedResults.Ok<IReadOnlyList<DlqEvent>>(rows);
    }

    private static async Task<Ok<DlqSummary>> Summary(
        EventSpineConsumerDbContext db,
        CancellationToken ct)
    {
        var byReason = await db.DlqEvents
            .AsNoTracking()
            .Where(e => e.ReplayedAt == null)
            .GroupBy(e => e.Reason)
            .Select(g => new { Reason = g.Key, Count = g.Count() })
            .ToDictionaryAsync(x => x.Reason, x => x.Count, ct);

        var totalPending = byReason.Values.Sum();
        var totalReplayed = await db.DlqEvents
            .AsNoTracking()
            .CountAsync(e => e.ReplayedAt != null, ct);

        return TypedResults.Ok(new DlqSummary(totalPending, totalReplayed, byReason));
    }

    private static async Task<Results<Ok<DlqEvent>, NotFound>> GetById(
        Guid id,
        EventSpineConsumerDbContext db,
        CancellationToken ct)
    {
        var entry = await db.DlqEvents.AsNoTracking()
            .FirstOrDefaultAsync(e => e.Id == id, ct);
        return entry is null ? TypedResults.NotFound() : TypedResults.Ok(entry);
    }

    private static async Task<Results<Ok<DlqEvent>, NotFound>> Replay(
        Guid id,
        DlqService dlq,
        CancellationToken ct)
    {
        var result = await dlq.ReplayAsync(id, ct);
        return result is null ? TypedResults.NotFound() : TypedResults.Ok(result);
    }
}

public sealed record DlqSummary(
    int TotalPending,
    int TotalReplayed,
    IReadOnlyDictionary<string, int> ByReason);

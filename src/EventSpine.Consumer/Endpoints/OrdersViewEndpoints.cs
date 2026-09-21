using EventSpine.Consumer.Data;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.EntityFrameworkCore;

namespace EventSpine.Consumer.Endpoints;

public static class OrdersViewEndpoints
{
    public static IEndpointRouteBuilder MapOrdersViewEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/orders").WithTags("orders-view");

        group.MapGet("/{id:guid}", GetById);
        group.MapGet("/", List);

        return app;
    }

    private static async Task<Results<Ok<OrderView>, NotFound>> GetById(
        Guid id,
        EventSpineConsumerDbContext db,
        CancellationToken ct)
    {
        var view = await db.OrdersView.AsNoTracking()
            .FirstOrDefaultAsync(v => v.Id == id, ct);
        return view is null ? TypedResults.NotFound() : TypedResults.Ok(view);
    }

    private static async Task<Ok<IReadOnlyList<OrderView>>> List(
        EventSpineConsumerDbContext db,
        CancellationToken ct)
    {
        var views = await db.OrdersView.AsNoTracking()
            .OrderByDescending(v => v.UpdatedAt)
            .Take(100)
            .ToListAsync(ct);
        return TypedResults.Ok<IReadOnlyList<OrderView>>(views);
    }
}

using EventSpine.Producer.Services;

namespace EventSpine.Producer.Endpoints;

/// <summary>HTTP endpoints do agregado Order. Producer-side; leituras públicas vão via projeção (Consumer).</summary>
public static class OrdersEndpoints
{
    public static IEndpointRouteBuilder MapOrdersEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/orders").WithTags("Orders");

        group.MapPost("/", async (CreateOrderRequest req, OrderService svc, CancellationToken ct) =>
        {
            if (req.Amount <= 0)
            {
                return Results.BadRequest(new { error = "amount must be positive" });
            }

            var order = await svc.CreateOrderAsync(req.Amount, ct);
            return Results.Created($"/orders/{order.Id}", ToDto(order));
        });

        group.MapPut("/{id:guid}/pay", async (Guid id, OrderService svc, CancellationToken ct) =>
        {
            try
            {
                var order = await svc.PayOrderAsync(id, ct);
                return Results.Ok(ToDto(order));
            }
            catch (KeyNotFoundException)
            {
                return Results.NotFound();
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new { error = ex.Message });
            }
        });

        group.MapGet("/{id:guid}", async (Guid id, OrderService svc, CancellationToken ct) =>
        {
            var order = await svc.GetOrderAsync(id, ct);
            return order is null ? Results.NotFound() : Results.Ok(ToDto(order));
        });

        return app;
    }

    private static object ToDto(Domain.Order order) => new
    {
        id = order.Id,
        amount = order.Amount,
        status = order.Status.ToString(),
        createdAt = order.CreatedAt,
        updatedAt = order.UpdatedAt,
    };
}

public sealed record CreateOrderRequest(decimal Amount);

// Producer scaffold — Bloco 2 vai preencher com:
//   - DbContext (orders + outbox)
//   - HTTP endpoints (POST /orders, PUT /orders/{id}/pay)
//   - OutboxRelay (BackgroundService que faz poll + produce Kafka)
//   - OpenTelemetry bootstrap
var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapGet("/", () => Results.Ok(new { service = "event-spine-producer", status = "scaffold" }));

app.Run();

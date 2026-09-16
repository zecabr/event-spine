// Consumer scaffold — Bloco 3 vai preencher com:
//   - DbContext (inbox + orders_view + dlq_events)
//   - ConsumerWorker (BackgroundService lendo do Kafka, dedupe via inbox, upsert projection)
//   - DLQ dashboard endpoints (GET /dlq, POST /dlq/{id}/replay)
//   - OpenTelemetry bootstrap
var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapGet("/", () => Results.Ok(new { service = "event-spine-consumer", status = "scaffold" }));

app.Run();

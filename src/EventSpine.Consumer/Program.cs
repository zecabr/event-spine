using EventSpine.Consumer.Data;
using EventSpine.Consumer.Dlq;
using EventSpine.Consumer.Endpoints;
using EventSpine.Consumer.Kafka;
using EventSpine.Consumer.Options;
using EventSpine.Consumer.Projection;
using EventSpine.Contracts.Validation;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// --- Data ---------------------------------------------------------------
builder.Services.AddDbContext<EventSpineConsumerDbContext>((sp, options) =>
{
    var connectionString = builder.Configuration.GetConnectionString("Postgres")
        ?? throw new InvalidOperationException("Missing ConnectionStrings:Postgres");
    options.UseNpgsql(connectionString);
});

// --- Options ------------------------------------------------------------
builder.Services.Configure<KafkaConsumerOptions>(
    builder.Configuration.GetSection("KafkaConsumer"));
builder.Services.Configure<RetryOptions>(
    builder.Configuration.GetSection("Retry"));

// --- Kafka producer (singleton — long-lived, thread-safe) ----------------
builder.Services.AddSingleton<IKafkaTopicProducer, KafkaTopicProducer>();

// --- Schema validator (singleton — schemas parsed once from embedded resources)
builder.Services.AddSingleton<SchemaValidator>();

// --- Projection + consumer + DLQ ----------------------------------------
builder.Services.AddScoped<OrderProjectionService>();
builder.Services.AddScoped<DlqService>();
builder.Services.AddHostedService<OrderEventsConsumer>();

var app = builder.Build();

// --- Bootstrap schema (idempotent) --------------------------------------
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<EventSpineConsumerDbContext>();
    await ConsumerDbInitializer.InitializeAsync(db);
}

// --- HTTP read-side + DLQ ops -------------------------------------------
app.MapOrdersViewEndpoints();
app.MapDlqEndpoints();
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();

public partial class Program;

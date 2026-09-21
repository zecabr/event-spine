using EventSpine.Consumer.Data;
using EventSpine.Consumer.Endpoints;
using EventSpine.Consumer.Kafka;
using EventSpine.Consumer.Options;
using EventSpine.Consumer.Projection;
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

// --- Projection + consumer ---------------------------------------------
builder.Services.AddScoped<OrderProjectionService>();
builder.Services.AddHostedService<OrderEventsConsumer>();

var app = builder.Build();

// --- Bootstrap schema (idempotent) --------------------------------------
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<EventSpineConsumerDbContext>();
    await ConsumerDbInitializer.InitializeAsync(db);
}

// --- HTTP read-side -----------------------------------------------------
app.MapOrdersViewEndpoints();
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();

// Marker for WebApplicationFactory<Program> in tests
public partial class Program;

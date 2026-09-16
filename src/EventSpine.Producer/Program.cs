using EventSpine.Producer.Data;
using EventSpine.Producer.Endpoints;
using EventSpine.Producer.Outbox;
using EventSpine.Producer.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// --- Configuração tipada ---
builder.Services.Configure<OutboxOptions>(builder.Configuration.GetSection(OutboxOptions.SectionName));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));

// --- Db context ---
var connectionString = builder.Configuration.GetConnectionString("Postgres")
    ?? throw new InvalidOperationException("Missing ConnectionStrings:Postgres");

builder.Services.AddDbContext<EventSpineDbContext>(opt => opt.UseNpgsql(connectionString));

// --- Serviços do domínio ---
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddScoped<OrderService>();

// --- Outbox pipeline ---
builder.Services.AddSingleton<KafkaEventPublisher>();
builder.Services.AddHostedService<OutboxRelay>();

var app = builder.Build();

// --- Schema (idempotente) na startup ---
await DbInitializer.EnsureCreatedAsync(connectionString, app.Lifetime.ApplicationStopping);

// --- Health check simples ---
app.MapGet("/", () => Results.Ok(new
{
    service = "event-spine-producer",
    status = "up",
    version = typeof(Program).Assembly.GetName().Version?.ToString() ?? "dev",
}));

// --- Domínio ---
app.MapOrdersEndpoints();

app.Run();

// Marker público pro WebApplicationFactory dos integration tests conseguir referenciar.
public partial class Program;

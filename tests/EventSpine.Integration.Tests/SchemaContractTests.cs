using Xunit;
using EventSpine.Contracts.Events;
using EventSpine.Contracts.Validation;
using System.Text.Json;

namespace EventSpine.Integration.Tests;

/// <summary>
/// Contract tests: the JSON schemas embedded in EventSpine.Contracts must
/// (a) load without error, (b) accept the payloads that the Producer actually
/// serializes today, and (c) reject payloads that are obviously wrong.
///
/// These live under Integration.Tests so they run in the same suite as the
/// rest of the pipeline, but they don't need any container — just the
/// embedded resources.
/// </summary>
public sealed class SchemaContractTests
{
    private readonly SchemaValidator _validator = new();

    [Fact]
    public void All_four_schemas_are_registered()
    {
        var known = SchemaValidator.KnownEventTypes.ToHashSet();
        Assert.Contains(nameof(OrderCreated), known);
        Assert.Contains(nameof(OrderPaid), known);
        Assert.Contains(nameof(OrderShipped), known);
        Assert.Contains(nameof(OrderCancelled), known);
    }

    [Fact]
    public void Real_OrderCreated_payload_validates()
    {
        var payload = JsonSerializer.Serialize(
            new OrderCreated(Guid.NewGuid(), 199.90m, DateTime.UtcNow));

        var result = _validator.Validate(nameof(OrderCreated), payload);

        Assert.True(result.IsValid, result.ErrorSummary);
    }

    [Fact]
    public void Real_OrderPaid_payload_validates()
    {
        var payload = JsonSerializer.Serialize(
            new OrderPaid(Guid.NewGuid(), 199.90m, DateTime.UtcNow));

        var result = _validator.Validate(nameof(OrderPaid), payload);

        Assert.True(result.IsValid, result.ErrorSummary);
    }

    [Fact]
    public void OrderCreated_with_negative_amount_fails_schema()
    {
        // Producer would never emit this — but a rogue publisher, a corrupted
        // migration, or a manual replay could. Schema is our defense.
        var payload = """
            { "OrderId": "3b0f5e50-6e26-4bdf-9c88-3a3b1a4c3f01",
              "Amount": -10,
              "OccurredAt": "2026-09-22T10:00:00Z" }
            """;

        var result = _validator.Validate(nameof(OrderCreated), payload);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Amount", StringComparison.OrdinalIgnoreCase)
                                          || e.Contains("exclusiveMinimum", StringComparison.OrdinalIgnoreCase)
                                          || e.Contains("minimum", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void OrderCreated_missing_required_field_fails_schema()
    {
        // Missing OccurredAt.
        var payload = """
            { "OrderId": "3b0f5e50-6e26-4bdf-9c88-3a3b1a4c3f01",
              "Amount": 100 }
            """;

        var result = _validator.Validate(nameof(OrderCreated), payload);

        Assert.False(result.IsValid);
    }

    [Fact]
    public void Unknown_event_type_returns_schema_not_found()
    {
        var result = _validator.Validate("SomeUnregisteredEvent", "{}");

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("No schema", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Malformed_JSON_fails_gracefully_without_throwing()
    {
        var result = _validator.Validate(nameof(OrderCreated), "{ not: valid json }");

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("not valid JSON", StringComparison.OrdinalIgnoreCase));
    }
}

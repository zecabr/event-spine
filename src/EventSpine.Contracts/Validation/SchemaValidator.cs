using EventSpine.Contracts.Events;
using Json.Schema;
using System.Reflection;
using System.Text.Json.Nodes;

namespace EventSpine.Contracts.Validation;

/// <summary>
/// Validates event payloads against JSON Schemas embedded in this assembly.
///
/// Design:
///   - Schemas are files in EventSpine.Contracts/Schemas/*.json compiled as
///     embedded resources — no file I/O at runtime, no external registry
///     service, no deployment coupling.
///   - The registry is a static map from event type name (matching
///     nameof(OrderCreated), etc.) to a parsed JsonSchema, built once and
///     reused for the process lifetime.
///   - Producer and consumer both use this class — same validator on both
///     sides means schema drift is caught early on either edge.
///
/// The value of "schema registry" here is not enforcement (records already
/// enforce shape at compile time in-process) but defense-in-depth: catches
/// payloads that arrive via wire from a rogue producer, a schema-forward
/// migration, or a manual reprocess.
/// </summary>
public sealed class SchemaValidator
{
    // Map: EventType name → parsed JsonSchema.
    private static readonly IReadOnlyDictionary<string, JsonSchema> Schemas = LoadEmbeddedSchemas();

    /// <summary>Registered event types — useful for tests and for /schemas endpoints.</summary>
    public static IReadOnlyCollection<string> KnownEventTypes => Schemas.Keys.ToList();

    /// <summary>
    /// Validates a serialized payload (as it lives inside
    /// <see cref="EventEnvelope.Payload"/>) against the schema for the given
    /// event type.
    /// </summary>
    public SchemaValidationResult Validate(string eventType, string payloadJson)
    {
        if (!Schemas.TryGetValue(eventType, out var schema))
            return SchemaValidationResult.SchemaNotFound(eventType);

        JsonNode? node;
        try
        {
            node = JsonNode.Parse(payloadJson);
        }
        catch (Exception ex)
        {
            return SchemaValidationResult.Invalid(eventType, new[] { $"Payload is not valid JSON: {ex.Message}" });
        }

        var options = new EvaluationOptions { OutputFormat = OutputFormat.List };
        var results = schema.Evaluate(node, options);

        if (results.IsValid) return SchemaValidationResult.Valid(eventType);

        var errors = FlattenErrors(results).ToList();
        return SchemaValidationResult.Invalid(eventType, errors);
    }

    private static IEnumerable<string> FlattenErrors(EvaluationResults results)
    {
        if (results.HasErrors && results.Errors is not null)
        {
            foreach (var kv in results.Errors)
                yield return $"{results.InstanceLocation}: {kv.Value}";
        }

        if (results.Details is null) yield break;
        foreach (var child in results.Details)
        foreach (var msg in FlattenErrors(child))
            yield return msg;
    }

    // --- Loading ------------------------------------------------------------

    private static IReadOnlyDictionary<string, JsonSchema> LoadEmbeddedSchemas()
    {
        // Map schema filename → C# type name that matches nameof(<EventRecord>).
        // Kept explicit so a rename of an event type without a rename of the
        // schema file is a compile break, not a silent registration miss.
        var mapping = new (string ResourceName, string EventType)[]
        {
            ("order-created.json",   nameof(OrderCreated)),
            ("order-paid.json",      nameof(OrderPaid)),
            ("order-shipped.json",   nameof(OrderShipped)),
            ("order-cancelled.json", nameof(OrderCancelled)),
        };

        var assembly = typeof(SchemaValidator).Assembly;
        var dict = new Dictionary<string, JsonSchema>(StringComparer.Ordinal);

        foreach (var (fileName, eventType) in mapping)
        {
            var resourceName = FindResourceName(assembly, fileName)
                ?? throw new InvalidOperationException(
                    $"Embedded schema '{fileName}' not found in {assembly.GetName().Name}. " +
                    "Check that Schemas/*.json is marked as <EmbeddedResource> in the csproj.");

            using var stream = assembly.GetManifestResourceStream(resourceName)!;
            var schema = JsonSchema.FromStream(stream).GetAwaiter().GetResult();
            dict[eventType] = schema;
        }

        return dict;
    }

    private static string? FindResourceName(Assembly assembly, string fileName)
    {
        // Manifest names include the default namespace + folder path with dots,
        // e.g. "EventSpine.Contracts.Schemas.order-created.json"
        foreach (var name in assembly.GetManifestResourceNames())
        {
            if (name.EndsWith(fileName, StringComparison.Ordinal))
                return name;
        }
        return null;
    }
}

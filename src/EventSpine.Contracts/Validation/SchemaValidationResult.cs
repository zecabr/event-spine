namespace EventSpine.Contracts.Validation;

/// <summary>
/// Outcome of validating a payload against its declared JSON schema.
///
/// Explicit result type (instead of throwing) because in the consumer path
/// we want to route schema violations to the DLQ, not blow up the pipeline.
/// </summary>
public sealed record SchemaValidationResult(
    bool IsValid,
    string EventType,
    IReadOnlyList<string> Errors)
{
    public static SchemaValidationResult Valid(string eventType)
        => new(true, eventType, Array.Empty<string>());

    public static SchemaValidationResult Invalid(string eventType, IReadOnlyList<string> errors)
        => new(false, eventType, errors);

    public static SchemaValidationResult SchemaNotFound(string eventType)
        => new(false, eventType, new[] { $"No schema registered for event type '{eventType}'." });

    public string ErrorSummary => IsValid ? string.Empty : string.Join("; ", Errors);
}

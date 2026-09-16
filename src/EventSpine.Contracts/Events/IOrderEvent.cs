namespace EventSpine.Contracts.Events;

/// <summary>
/// Marker de todos os eventos do agregado Order. Serve pra: (a) restringir tipos
/// que o Producer aceita serializar no outbox; (b) descoberta via reflection na
/// deserialização do Consumer no Bloco 3.
/// </summary>
public interface IOrderEvent
{
    /// <summary>Id do agregado — usado como Kafka key pra garantir ordem por Order.</summary>
    Guid OrderId { get; }
}

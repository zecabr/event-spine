using EventSpine.Producer.Domain;
using Microsoft.EntityFrameworkCore;

namespace EventSpine.Producer.Data;

/// <summary>DbContext do Producer: agregado <c>orders</c> + tabela <c>outbox</c>.</summary>
public sealed class EventSpineDbContext : DbContext
{
    public EventSpineDbContext(DbContextOptions<EventSpineDbContext> options)
        : base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(e =>
        {
            e.ToTable("orders");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Amount).HasColumnName("amount").HasColumnType("numeric(18,2)");
            e.Property(x => x.Status)
                .HasColumnName("status")
                .HasConversion<string>()
                .HasMaxLength(32);
            e.Property(x => x.CreatedAt).HasColumnName("created_at");
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at");
        });

        modelBuilder.Entity<OutboxMessage>(e =>
        {
            e.ToTable("outbox");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.EventType).HasColumnName("event_type").HasMaxLength(64);
            e.Property(x => x.AggregateId).HasColumnName("aggregate_id");
            e.Property(x => x.OccurredAt).HasColumnName("occurred_at");
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("jsonb");
            e.Property(x => x.SentAt).HasColumnName("sent_at");
            // Index parcial pro polling: só linhas não enviadas.
            e.HasIndex(x => x.Id)
                .HasFilter("sent_at IS NULL")
                .HasDatabaseName("ix_outbox_pending");
        });
    }
}

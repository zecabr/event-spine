using Microsoft.EntityFrameworkCore;

namespace EventSpine.Consumer.Data;

public sealed class EventSpineConsumerDbContext : DbContext
{
    public EventSpineConsumerDbContext(DbContextOptions<EventSpineConsumerDbContext> options)
        : base(options)
    {
    }

    public DbSet<ConsumerInboxEntry> ConsumerInbox => Set<ConsumerInboxEntry>();
    public DbSet<OrderView> OrdersView => Set<OrderView>();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<ConsumerInboxEntry>(e =>
        {
            e.ToTable("consumer_inbox");
            e.HasKey(x => x.EventId);
            e.Property(x => x.EventId).HasColumnName("event_id");
            e.Property(x => x.ProcessedAt).HasColumnName("processed_at");
        });

        mb.Entity<OrderView>(e =>
        {
            e.ToTable("orders_view");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Amount).HasColumnName("amount").HasColumnType("numeric(18,2)");
            e.Property(x => x.Status).HasColumnName("status").HasMaxLength(32).IsRequired();
            e.Property(x => x.CreatedAt).HasColumnName("created_at");
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at");
            e.Property(x => x.Version).HasColumnName("version");
        });
    }
}

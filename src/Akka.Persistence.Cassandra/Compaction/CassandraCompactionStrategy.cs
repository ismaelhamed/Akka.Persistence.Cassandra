using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    public interface ICassandraCompactionStrategy
    {
        // ReSharper disable once InconsistentNaming
        string AsCQL { get; }
    }

    public static class CassandraCompactionStrategy
    {
        public static ICassandraCompactionStrategy Create(Config config)
        {
            return BaseCompactionStrategy.FromConfig(config);
        }
    }
}
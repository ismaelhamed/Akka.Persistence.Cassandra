using System.Collections.Immutable;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    public interface ICassandraCompactionStrategyConfig<out TStrategy> where TStrategy : ICassandraCompactionStrategy
    {
        string TypeName { get; }
        IImmutableList<string> PropertyKeys { get; }
        TStrategy FromConfig(Config config);
    }
}
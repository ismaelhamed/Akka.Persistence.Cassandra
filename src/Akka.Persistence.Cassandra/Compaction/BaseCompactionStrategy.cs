using System;
using System.Collections.Immutable;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    // Based upon https://github.com/apache/cassandra/blob/cassandra-2.2/src/java/org/apache/cassandra/db/compaction/AbstractCompactionStrategy.java
    public abstract class BaseCompactionStrategy : ICassandraCompactionStrategy
    {
        protected BaseCompactionStrategy(Config config)
        {
            var enabled = config.GetBoolean("enabled", true);
            var tombstoneCompactionInterval = config.GetLong("tombstone_compaction_interval", 86400L);
            var tombstoneThreshold = config.GetDouble("tombstone_threshold", 0.2);
            var uncheckedTombstoneCompaction = config.GetBoolean("unchecked_tombstone_compaction");

            if (tombstoneCompactionInterval <= 0)
                throw new ArgumentException($"tombstone_compaction_interval must be greater than 0, but was {tombstoneCompactionInterval}");
            if (tombstoneThreshold <= 0)
                throw new ArgumentException($"tombstone_threshold must be greater than 0, but was {tombstoneThreshold}");

            AsCQL = $@"
'enabled' : {enabled.ToString().ToLowerInvariant()},
'tombstone_compaction_interval' : {tombstoneCompactionInterval},
'tombstone_threshold' : {tombstoneThreshold},
'unchecked_tombstone_compaction' : {uncheckedTombstoneCompaction.ToString().ToLowerInvariant()}";
        }

        public string AsCQL { get; protected set; }

        public static ICassandraCompactionStrategy FromConfig(Config config)
        {
            return BaseCompactionStrategyConfig.Instance.FromConfig(config);
        }
    }

    public class BaseCompactionStrategyConfig : ICassandraCompactionStrategyConfig<BaseCompactionStrategy>
    {
        private BaseCompactionStrategyConfig()
        {
        }

        public static BaseCompactionStrategyConfig Instance { get; } = new BaseCompactionStrategyConfig();

        public string TypeName { get; } = "BaseCompactionStrategy";

        public IImmutableList<string> PropertyKeys { get; } = new []
        {
            "class",
            "enabled",
            "tombstone_compaction_interval",
            "tombstone_threshold",
            "unchecked_tombstone_compaction"
        }.ToImmutableList();

        public BaseCompactionStrategy FromConfig(Config config)
        {
            var typeName = config.HasPath("class") ? config.GetString("class") : string.Empty;

            if (typeName.Equals(DateTieredCompactionStrategyConfig.Instance.TypeName))
                return DateTieredCompactionStrategyConfig.Instance.FromConfig(config);
            if (typeName.Equals(LeveledCompactionStrategyConfig.Instance.TypeName))
                return LeveledCompactionStrategyConfig.Instance.FromConfig(config);
            if (typeName.Equals(SizeTieredCompactionStrategyConfig.Instance.TypeName))
                return SizeTieredCompactionStrategyConfig.Instance.FromConfig(config);

            return
                SizeTieredCompactionStrategyConfig.Instance.FromConfig(
                    ConfigurationFactory.ParseString(
                        $"class = \"{SizeTieredCompactionStrategyConfig.Instance.TypeName}\""));
        }
    }
}
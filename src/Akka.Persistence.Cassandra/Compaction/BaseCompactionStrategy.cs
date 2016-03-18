//-----------------------------------------------------------------------
// <copyright file="BaseCompactionStrategy.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

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
            Enabled = config.GetBoolean("enabled", true);
            TombstoneCompactionInterval = config.GetLong("tombstone_compaction_interval", 86400L);
            TombstoneThreshold = config.GetDouble("tombstone_threshold", 0.2);
            UncheckedTombstoneCompaction = config.GetBoolean("unchecked_tombstone_compaction");

            if (TombstoneCompactionInterval <= 0)
                throw new ArgumentException($"tombstone_compaction_interval must be greater than 0, but was {TombstoneCompactionInterval}");
            if (TombstoneThreshold <= 0)
                throw new ArgumentException($"tombstone_threshold must be greater than 0, but was {TombstoneThreshold}");

            AsCql = $@"
'enabled' : {Enabled.ToString().ToLowerInvariant()},
'tombstone_compaction_interval' : {TombstoneCompactionInterval},
'tombstone_threshold' : {TombstoneThreshold},
'unchecked_tombstone_compaction' : {UncheckedTombstoneCompaction.ToString().ToLowerInvariant()}".Trim();
        }

        public bool Enabled { get; }
        public long TombstoneCompactionInterval { get; }
        public double TombstoneThreshold { get; }
        public bool UncheckedTombstoneCompaction { get; }

        public string AsCql { get; protected set; }

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
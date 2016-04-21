using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    public class LeveledCompactionStrategy : BaseCompactionStrategy
    {
        public LeveledCompactionStrategy(Config config) : base(config)
        {
            if (!LeveledCompactionStrategyConfig.Instance.TypeName.Equals(config.GetString("class")))
                throw new ArgumentException(
                    $"Config does not specify a {LeveledCompactionStrategyConfig.Instance.TypeName}");
            if (config.AsEnumerable().Select(kvp => kvp.Key).Any(k => !LeveledCompactionStrategyConfig.Instance.PropertyKeys.Contains(k)))
                throw new ArgumentException(
                    $"Config contains properties not supported by a {LeveledCompactionStrategyConfig.Instance.TypeName}");

            // ReSharper disable once InconsistentNaming
            var ssTableSizeInMB = config.GetLong("sstable_size_in_mb", 160);

            if (ssTableSizeInMB <= 0)
                throw new ArgumentException($"sstable_size_in_mb must be greater than 0, but was {ssTableSizeInMB}");

            AsCQL = $@"{{
'class' : '{LeveledCompactionStrategyConfig.Instance.TypeName}',
{AsCQL},
'sstable_size_in_mb' : {ssTableSizeInMB}
}}";
        }
    }

    public class LeveledCompactionStrategyConfig : ICassandraCompactionStrategyConfig<LeveledCompactionStrategy>
    {
        private LeveledCompactionStrategyConfig()
        {
        }

        public static LeveledCompactionStrategyConfig Instance { get; } = new LeveledCompactionStrategyConfig();

        public string TypeName { get; } = "LeveledCompactionStrategy";

        public IImmutableList<string> PropertyKeys { get; } = BaseCompactionStrategyConfig.Instance.PropertyKeys.Union(new []
        {
            "sstable_size_in_mbs"
        }).ToImmutableList();

        public LeveledCompactionStrategy FromConfig(Config config)
        {
            return new LeveledCompactionStrategy(config);
        }
    }
}
using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    public class SizeTieredCompactionStrategy : BaseCompactionStrategy
    {
        public SizeTieredCompactionStrategy(Config config) : base(config)
        {
            if (!SizeTieredCompactionStrategyConfig.Instance.TypeName.Equals(config.GetString("class")))
                throw new ArgumentException(
                    $"Config does not specify a {SizeTieredCompactionStrategyConfig.Instance.TypeName}");
            if (config.AsEnumerable().Select(kvp => kvp.Key).Any(k => !SizeTieredCompactionStrategyConfig.Instance.PropertyKeys.Contains(k)))
                throw new ArgumentException(
                    $"Config contains properties not supported by a {SizeTieredCompactionStrategyConfig.Instance.TypeName}");

            var bucketHigh = config.GetDouble("bucket_high", 1.5);
            var bucketLow = config.GetDouble("bucket_low", 0.5);
            var maxThreshold = config.GetInt("max_threshold", 32);
            var minThreshold = config.GetInt("min_threshold", 4);
            // ReSharper disable once InconsistentNaming
            var minSSTableSize = config.GetLong("min_sstable_size", 50);

            if (bucketHigh <= bucketLow)
                throw new ArgumentException($"bucket_high must be greater than bucket_low, but was {bucketHigh}");
            if (maxThreshold <= 0)
                throw new ArgumentException($"max_threshold must be greater than 0, but was {maxThreshold}");
            if (minThreshold <= 1)
                throw new ArgumentException($"min_threshold must be greater than 1, but was {minThreshold}");
            if (minThreshold >= maxThreshold)
                throw new ArgumentException($"max_threshold must be larger than min_threshold, but was {maxThreshold}");
            if (minSSTableSize <= 0)
                throw new ArgumentException($"min_sstable_size must be greater than 0, but was {minSSTableSize}");

            AsCQL = $@"{{
'class' : '{SizeTieredCompactionStrategyConfig.Instance.TypeName}',
{AsCQL},
'bucket_high' : {bucketHigh},
'bucket_low' : {bucketLow},
'max_threshold' : {maxThreshold},
'min_threshold' : {minThreshold},
'min_sstable_size' : {minSSTableSize}
}}";
        }
    }

    public class SizeTieredCompactionStrategyConfig : ICassandraCompactionStrategyConfig<SizeTieredCompactionStrategy>
    {
        private SizeTieredCompactionStrategyConfig()
        {
        }

        public static SizeTieredCompactionStrategyConfig Instance { get; } = new SizeTieredCompactionStrategyConfig();

        public string TypeName { get; } = "SizeTieredCompactionStrategy";

        public IImmutableList<string> PropertyKeys { get; } = BaseCompactionStrategyConfig.Instance.PropertyKeys.Union(new []
        {
            "bucket_high",
            "bucket_low",
            "max_threshold",
            "min_threshold",
            "min_sstable_size"
        }).ToImmutableList();

        public SizeTieredCompactionStrategy FromConfig(Config config)
        {
            return new SizeTieredCompactionStrategy(config);
        }
    }
}
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

            BucketHigh = config.GetDouble("bucket_high", 1.5);
            BucketLow = config.GetDouble("bucket_low", 0.5);
            MaxThreshold = config.GetInt("max_threshold", 32);
            MinThreshold = config.GetInt("min_threshold", 4);
            MinSSTableSize = config.GetLong("min_sstable_size", 50);

            if (BucketHigh <= BucketLow)
                throw new ArgumentException($"bucket_high must be greater than bucket_low, but was {BucketHigh}");
            if (MaxThreshold <= 0)
                throw new ArgumentException($"max_threshold must be greater than 0, but was {MaxThreshold}");
            if (MinThreshold <= 1)
                throw new ArgumentException($"min_threshold must be greater than 1, but was {MinThreshold}");
            if (MinThreshold >= MaxThreshold)
                throw new ArgumentException($"max_threshold must be larger than min_threshold, but was {MaxThreshold}");
            if (MinSSTableSize <= 0)
                throw new ArgumentException($"min_sstable_size must be greater than 0, but was {MinSSTableSize}");

            AsCQL = $@"{{
'class' : '{SizeTieredCompactionStrategyConfig.Instance.TypeName}',
{AsCQL},
'bucket_high' : {BucketHigh},
'bucket_low' : {BucketLow},
'max_threshold' : {MaxThreshold},
'min_threshold' : {MinThreshold},
'min_sstable_size' : {MinSSTableSize}
}}";
        }

        public double BucketHigh { get; }
        public double BucketLow { get; }
        public int MaxThreshold { get; }
        public int MinThreshold { get; }
        // ReSharper disable once InconsistentNaming
        public long MinSSTableSize { get; }
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
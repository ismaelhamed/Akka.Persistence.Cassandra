using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    public class DateTieredCompactionStrategy : BaseCompactionStrategy
    {
        public DateTieredCompactionStrategy(Config config) : base(config)
        {
            if (!DateTieredCompactionStrategyConfig.Instance.TypeName.Equals(config.GetString("class")))
                throw new ArgumentException(
                    $"Config does not specify a {DateTieredCompactionStrategyConfig.Instance.TypeName}");
            if (config.AsEnumerable().Select(kvp => kvp.Key).Any(k => !DateTieredCompactionStrategyConfig.Instance.PropertyKeys.Contains(k)))
                throw new ArgumentException(
                    $"Config contains properties not supported by a {DateTieredCompactionStrategyConfig.Instance.TypeName}");

            var baseTimeSeconds = config.GetLong("base_time_seconds", 3600);
            // ReSharper disable once InconsistentNaming
            var maxSSTableAgeDays = config.GetInt("max_sstable_age_days", 365);
            var maxThreshold = config.GetInt("max_threshold", 32);
            var minThreshold = config.GetInt("min_threshold", 4);
            var timestampResolution = config.GetString("timestamp_resolution", "MICROSECONDS").ToUpperInvariant();

            if (baseTimeSeconds <= 0)
                throw new ArgumentException($"base_time_seconds must be greater than 0, but was {baseTimeSeconds}");
            if (maxSSTableAgeDays < 0)
                throw new ArgumentException($"max_sstable_age_days must be greater than or equal to 0, but was {maxSSTableAgeDays}");
            if (maxThreshold <= 0)
                throw new ArgumentException($"max_threshold must be greater than 0, but was {maxThreshold}");
            if (minThreshold <= 1)
                throw new ArgumentException($"min_threshold must be greater than 1, but was {minThreshold}");
            if (minThreshold >= maxThreshold)
                throw new ArgumentException($"max_threshold must be larger than min_threshold, but was {maxThreshold}");
            if (!timestampResolution.Equals("MICROSECONDS", StringComparison.InvariantCulture) || !timestampResolution.Equals("MILLISECONDS", StringComparison.InvariantCulture))
                throw new ArgumentException($"timestamp_resolution {timestampResolution} is not valid");

            AsCQL = $@"{{
'class' : '{DateTieredCompactionStrategyConfig.Instance.TypeName}',
{AsCQL},
'base_time_seconds' : {baseTimeSeconds},
'max_sstable_age_days' : {maxSSTableAgeDays},
'max_threshold' : {maxThreshold},
'min_threshold' : {minThreshold},
'timestamp_resolution' : '{timestampResolution}'
}}";
        }
    }

    public class DateTieredCompactionStrategyConfig : ICassandraCompactionStrategyConfig<DateTieredCompactionStrategy>
    {
        private DateTieredCompactionStrategyConfig()
        {
        }

        public static DateTieredCompactionStrategyConfig Instance { get; } = new DateTieredCompactionStrategyConfig();

        public string TypeName { get; } = "DateTieredCompactionStrategy";

        public IImmutableList<string> PropertyKeys { get; } = BaseCompactionStrategyConfig.Instance.PropertyKeys.Union(new []
        {
            "base_time_seconds",
            "max_sstable_age_days",
            "max_threshold",
            "min_threshold",
            "timestamp_resolution"
        }).ToImmutableList();

        public DateTieredCompactionStrategy FromConfig(Config config)
        {
            return new DateTieredCompactionStrategy(config);
        }
    }
}
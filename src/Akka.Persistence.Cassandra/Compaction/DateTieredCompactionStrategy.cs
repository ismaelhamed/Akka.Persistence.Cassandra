//-----------------------------------------------------------------------
// <copyright file="DateTieredCompactionStrategy.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

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

            BaseTimeSeconds = config.GetLong("base_time_seconds", 3600);
            MaxSSTableAgeDays = config.GetInt("max_sstable_age_days", 365);
            MaxThreshold = config.GetInt("max_threshold", 32);
            MinThreshold = config.GetInt("min_threshold", 4);
            TimestampResolution = config.GetString("timestamp_resolution", "MICROSECONDS").ToUpperInvariant();

            if (BaseTimeSeconds <= 0)
                throw new ArgumentException($"base_time_seconds must be greater than 0, but was {BaseTimeSeconds}");
            if (MaxSSTableAgeDays < 0)
                throw new ArgumentException($"max_sstable_age_days must be greater than or equal to 0, but was {MaxSSTableAgeDays}");
            if (MaxThreshold <= 0)
                throw new ArgumentException($"max_threshold must be greater than 0, but was {MaxThreshold}");
            if (MinThreshold <= 1)
                throw new ArgumentException($"min_threshold must be greater than 1, but was {MinThreshold}");
            if (MinThreshold >= MaxThreshold)
                throw new ArgumentException($"max_threshold must be larger than min_threshold, but was {MaxThreshold}");
            if (!TimestampResolution.Equals("MICROSECONDS", StringComparison.InvariantCulture) && !TimestampResolution.Equals("MILLISECONDS", StringComparison.InvariantCulture))
                throw new ArgumentException($"timestamp_resolution {TimestampResolution} is not valid");

            AsCql = $@"{{
'class' : '{DateTieredCompactionStrategyConfig.Instance.TypeName}',
{AsCql},
'base_time_seconds' : {BaseTimeSeconds},
'max_sstable_age_days' : {MaxSSTableAgeDays},
'max_threshold' : {MaxThreshold},
'min_threshold' : {MinThreshold},
'timestamp_resolution' : '{TimestampResolution}'
}}";
        }

        public long BaseTimeSeconds { get; }
        // ReSharper disable once InconsistentNaming
        public int MaxSSTableAgeDays { get; }
        public int MaxThreshold { get; }
        public int MinThreshold { get; }
        public string TimestampResolution { get; }
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
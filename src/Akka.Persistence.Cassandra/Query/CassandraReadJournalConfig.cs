using System;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Journal;
using Cassandra;

namespace Akka.Persistence.Cassandra.Query
{
    internal class CassandraReadJournalConfig
    {
        public CassandraReadJournalConfig(Config config, CassandraJournalConfig writePluginConfig)
        {
            RefreshInterval = config.GetTimeSpan("refresh-interval", null, false);
            MaxBufferSize = config.GetInt("max-buffer-size");
            FetchSize = config.GetInt("max-result-size-query");
            ReadConsistency = (ConsistencyLevel) Enum.Parse(typeof(ConsistencyLevel), config.GetString("read-consistency"), true);
            FirstTimeBucket = new TimeBucket(config.GetString("first-time-bucket"));
            EventualConsistencyDelay = config.GetTimeSpan("eventual-consistency-delay", null, false);
            DelayedEventTimeout = config.GetTimeSpan("delayed-event-timeout", null, false);
            PluginDispatcher = config.GetString("plugin-dispatcher");

            EventsByTagView = writePluginConfig.EventsByTagView;
            Keyspace = writePluginConfig.Keyspace;
            TargetPartitionSize = writePluginConfig.TargetPartitionSize;
            Table = writePluginConfig.Table;
            PubsubMinimumInterval = writePluginConfig.PubsubMinimumInterval;
        }

        public TimeSpan RefreshInterval { get; }
        public int MaxBufferSize { get; }
        public int FetchSize { get; }
        public ConsistencyLevel ReadConsistency { get; }
        public TimeBucket FirstTimeBucket { get; }
        public TimeSpan EventualConsistencyDelay { get; }
        public TimeSpan DelayedEventTimeout { get; }
        public string PluginDispatcher { get; }

        public string EventsByTagView { get; }
        public string Keyspace { get; }
        public int TargetPartitionSize { get; }
        public string Table { get; }
        public TimeSpan? PubsubMinimumInterval { get; }
    }
}
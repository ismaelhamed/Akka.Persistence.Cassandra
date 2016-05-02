using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Configuration;


namespace Akka.Persistence.Cassandra.Journal
{
    /// <summary>
    /// Settings for the Cassandra journal implementation, parsed from HOCON configuration.
    /// </summary>
    public class CassandraJournalConfig : CassandraPluginConfig
    {
        public const string TargetPartitionProperty = "target-partition-size";

        public CassandraJournalConfig(ActorSystem system, Config config)
            : base(system, config)
        {
            TargetPartitionSize = config.GetInt(TargetPartitionProperty);
            MaxResultSize = config.GetInt("max-result-size");
            ReplayMaxResultSize = config.GetInt("max-result-size-replay");
            GcGraceSeconds = config.GetLong("gc-grace-seconds");
            MaxMessageBatchSize = config.GetInt("max-message-batch-size");
            DeleteRetries = config.GetInt("delete-retries");
            WriteRetries = config.GetInt("write-retries");
            Cassandra2XCompat = config.GetBoolean("cassandra-2x-compat");
            EnableEventsByTagQuery = !Cassandra2XCompat && config.GetBoolean("enable-events-by-tag-query");
            EventsByTagView = config.GetString("events-by-tag-view");
            QueryPlugin = config.GetString("query-plugin");
            PubsubMinimumInterval = GetPubsubMinimumInterval(config);
            Tags = config.GetConfig("tags").AsEnumerable().Select(entry =>
            {
                var val = entry.Value.GetString();
                int tagId;
                var tag = entry.Key;
                int.TryParse(val, out tagId);
                if (!(1 <= tagId && tagId <= 3))
                    throw new NotSupportedException(
                        $"Tag identifer for [{tag}] must be a 1, 2, or 3, was [{tagId}].Max {MaxTagsPerEvent} tags per event is supported.");
                return new KeyValuePair<string, int>(tag, tagId);
            }).ToDictionary(kvp => kvp.Key, kvp => kvp.Value).ToImmutableDictionary();

            MaxTagId = !EnableEventsByTagQuery ? 0 : (Tags.Count == 0 ? 1 : Tags.Values.Max());
        }

        /// <summary>
        /// The approximate number of rows per partition to use. Cannot be changed after table creation.
        /// </summary>
        public int TargetPartitionSize { get; }

        /// <summary>
        /// The maximum number of messages to retrieve in one request when replaying messages.
        /// </summary>
        public int MaxResultSize { get; }

        /// <summary>
        /// Maximum size of result set during replay
        /// </summary>
        public int ReplayMaxResultSize { get; }

        /// <summary>
        /// The time to wait before cassandra will remove the thombstones created for deleted entries.
        /// </summary>
        public long GcGraceSeconds { get; }

        /// <summary>
        /// Maximum number of messages that will be batched when using `persistAsync`. 
        /// Also used as the max batch size for deletes.
        /// </summary>
        public int MaxMessageBatchSize { get; }

        /// <summary>
        ///  Number of retries before giving up
        /// </summary>
        public int DeleteRetries { get; }

        /// <summary>
        /// The number of retries when a write request returns a TimeoutException or an UnavailableException.
        /// </summary>
        public int WriteRetries { get; }

        /// <summary>
        /// Set this to true to only use Cassandra 2.x compatible features,
        /// </summary>
        public bool Cassandra2XCompat { get; }

        /// <summary>
        /// Possibility to disable the eventsByTag query and creation of
        /// the materialized view. This will automatically be off when
        /// <see cref="Cassandra2XCompat"/> is on.
        /// </summary>
        public bool EnableEventsByTagQuery { get; }

        /// <summary>
        /// Name of the materialized view for EventsByTag query
        /// </summary>
        public string EventsByTagView { get; }

        /// <summary>
        /// The query journal to use when recoverying
        /// </summary>
        public string QueryPlugin { get; }

        /// <summary>
        /// <para>
        /// Minimum time between publishing messages to <see cref="DistributedPubSub"/> to announce events for a specific tag have
        /// been written. These announcements cause any ongoing getEventsByTag to immediately re-poll, rather than
        /// wait. In order enable this feature, make the following settings:
        /// <list type="bullet">
        /// <item><description>enable clustering for your actor system</description></item>
        /// <item><description>cassandra-journal.pubsub-minimum-interval = 1s (send real-time announcements at most every sec)</description></item>
        /// <item><description>cassandra-query-journal.eventual-consistency-delay = 0s (so it immediately tries to show changes)</description></item>
        /// </list> 
        /// </para>
        /// <para>
        /// Setting pubsub-minimum-interval to "off" will disable the journal sending these announcements. 
        /// </para>
        /// </summary>
        public TimeSpan? PubsubMinimumInterval { get; }

        public int MaxTagsPerEvent => 3;

        /// <summary>
        /// Tag identifier mapping
        /// </summary>
        public IImmutableDictionary<string, int> Tags { get; }

        /// <summary>
        /// Will be 0 if <see cref="EnableEventsByTagQuery"/> is disabled,
        /// will be 1 if <see cref="Tags"/> is empty, otherwise the number of configured
        /// distinct tag identifiers.
        /// </summary>
        public int MaxTagId { get; }

        private static TimeSpan? GetPubsubMinimumInterval(Config config)
        {
            const string key = "pubsub-minimum-interval";
            var val = config.GetString(key).ToLowerInvariant();
            if ("off".Equals(val))
                return null;
            var result = config.GetTimeSpan(key, null, false);
            if (result <= TimeSpan.Zero)
                throw new ArgumentException($"{key} must be greater than 0, or 'off'");
            return result;
        }

    }
}
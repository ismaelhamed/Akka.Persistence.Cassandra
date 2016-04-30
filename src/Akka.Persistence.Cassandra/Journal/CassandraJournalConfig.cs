using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;


namespace Akka.Persistence.Cassandra.Journal
{
    /// <summary>
    /// Settings for the Cassandra journal implementation, parsed from HOCON configuration.
    /// </summary>
    public class CassandraJournalConfig : CassandraPluginConfig
    {
        public const string TargetPartitionProperty = "target-partition-size";

        /// <summary>
        /// The approximate number of rows per partition to use. Cannot be changed after table creation.
        /// </summary>
        public int TargetPartitionSize { get; private set; }

        /// <summary>
        /// The maximum number of messages to retrieve in one request when replaying messages.
        /// </summary>
        public int MaxResultSize { get; private set; }

        /// <summary>
        ///  Number of retries before giving up
        /// </summary>
        public int DeleteRetries { get; private set; }

        /// <summary>
        /// The number of retries when a write request returns a TimeoutException or an UnavailableException.
        /// </summary>
        public int WriteRetries { get; private set; }

        public long GcGraceSeconds { get; }

        public bool Cassandra2XCompat { get; }

        /// <summary>
        /// Maximum number of messages that will be batched when using `persistAsync`. 
        /// Also used as the max batch size for deletes.
        /// </summary>
        public int MaxMessageBatchSize { get; private set; }

        /// <summary>
        /// Maximum size of result set during replay
        /// </summary>
        public int ReplayMaxResultSize { get; private set; }

        public int MaxTagsPerEvent => 3;

        public IReadOnlyDictionary<string, int> Tags { get; }

        public bool EnableEventsByTagQuery { get; }

        public string EventsByTagView { get; }

        public string QueryPlugin { get; }

        public TimeSpan? PubsubMinimumInterval { get; }

        /// <summary>
        /// Will be 0 if <see cref="EnableEventsByTagQuery"/> is disabled,
        /// will be 1 if <see cref="Tags"/> is empty, otherwise the number of configured
        /// distinct tag identifiers.
        /// </summary>
        public int MaxTagId { get; }

        public CassandraJournalConfig(ActorSystem system, Config config)
            : base(system, config)
        {
            TargetPartitionSize = config.GetInt(TargetPartitionProperty);
            MaxResultSize = config.GetInt("max-result-size"); // TODO: not used in the scala version?...
            DeleteRetries = config.GetInt("delete-retries");
            WriteRetries = config.GetInt("write-retries");
            GcGraceSeconds = config.GetLong("gc-grace-seconds");
            Cassandra2XCompat = config.GetBoolean("cassandra-2x-compat");
            MaxMessageBatchSize = config.GetInt("max-message-batch-size");
            ReplayMaxResultSize = config.GetInt("max-result-size-replay");
            var tags = new Dictionary<string, int>();
            foreach (var entry in config.GetConfig("tags").AsEnumerable())
            {
                var val = entry.Value.GetString();
                int tagId;
                var tag = entry.Key;
                int.TryParse(val, out tagId);
                if ( !(1 <= tagId && tagId <= 3) )
                    throw new NotSupportedException($"Tag identifer for [{tag}] must be a 1, 2, or 3, was [{tagId}].Max {MaxTagsPerEvent} tags per event is supported.");
                tags.Add(tag, tagId);
            }
            Tags = new ReadOnlyDictionary<string,int>(tags);
            EventsByTagView = config.GetString("events-by-tag-view");
            PubsubMinimumInterval = GetPubsubMinimumInterval(config);

            EnableEventsByTagQuery = !Cassandra2XCompat && config.GetBoolean("enable-events-by-tag-query");
            QueryPlugin = config.GetString("query-plugin");

            MaxTagId = !EnableEventsByTagQuery ? 0 : (Tags.Count == 0 ? 1 : Tags.Values.Max());
        }

        private TimeSpan? GetPubsubMinimumInterval(Config config)
        {
            var key = "pubsub-minimum-interval";
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
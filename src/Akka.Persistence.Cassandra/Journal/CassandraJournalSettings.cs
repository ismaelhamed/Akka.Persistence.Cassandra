using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Akka.Actor;
using Akka.Configuration;


namespace Akka.Persistence.Cassandra.Journal
{
    /// <summary>
    /// Settings for the Cassandra journal implementation, parsed from HOCON configuration.
    /// </summary>
    public class CassandraJournalSettings : CassandraSettings
    {
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

        /// <summary>
        /// Maximum number of messages that will be batched when using `persistAsync`. 
        /// Also used as the max batch size for deletes.
        /// </summary>
        public int MaxMessageBatchSize { get; private set; }

        /// <summary>
        /// Maximum size of result set during replay
        /// </summary>
        public int MaxResultSizeReplay { get; private set; }

        public int MaxTagsPerEvent => 3;

        public IReadOnlyDictionary<string, int> Tags { get; private set; }

        public string EventsByTagView { get; }

        public TimeSpan? PubsubMinimumInterval { get; }

        public CassandraJournalSettings(ActorSystem system, Config config)
            : base(system, config)
        {
            TargetPartitionSize = config.GetInt("target-partition-size");
            MaxResultSize = config.GetInt("max-result-size"); // TODO: not used in the scala version?...
            DeleteRetries = config.GetInt("delete-retries");
            WriteRetries = config.GetInt("write-retries");
            MaxMessageBatchSize = config.GetInt("max-message-batch-size");
            MaxResultSizeReplay = config.GetInt("max-result-size-replay");
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
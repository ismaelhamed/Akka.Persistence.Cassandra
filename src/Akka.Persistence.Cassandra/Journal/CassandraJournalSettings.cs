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
        public long TargetPartitionSize { get; private set; }

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

        public CassandraJournalSettings(Config config)
            : base(config)
        {
            TargetPartitionSize = config.GetLong("target-partition-size");
            MaxResultSize = config.GetInt("max-result-size"); // TODO: not used in the scala version?...
            DeleteRetries = config.GetInt("delete-retries");
            WriteRetries = config.GetInt("write-retries");
            MaxMessageBatchSize = config.GetInt("max-message-batch-size");
            MaxResultSizeReplay = config.GetInt("max-result-size-replay");
        }

    }
}
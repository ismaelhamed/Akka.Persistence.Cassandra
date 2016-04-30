using Akka.Actor;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Snapshot
{
    /// <summary>
    /// Settings for the Cassandra snapshot store implementation, parsed from HOCON configuration.
    /// </summary>
    public class CassandraSnapshotStoreConfig : CassandraPluginConfig
    {
        /// <summary>
        /// The maximum number of snapshot metadata records to retrieve in a single request when trying to find
        /// snapshots that meet criteria.
        /// </summary>
        public int MaxMetadataResultSize { get; private set; }

        public CassandraSnapshotStoreConfig(ActorSystem system, Config config) 
            : base(system, config)
        {
            MaxMetadataResultSize = config.GetInt("max-metadata-result-size");
        }
    }
}

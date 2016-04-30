using System;
using Akka.Actor;
using Akka.Persistence.Cassandra.Journal;
using Akka.Persistence.Cassandra.Snapshot;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// An Akka.NET extension for Cassandra persistence.
    /// </summary>
    public class CassandraExtension : IExtension
    {
        /// <summary>
        /// The settings for the Cassandra journal.
        /// </summary>
        public CassandraJournalConfig JournalConfig { get; private set; }

        /// <summary>
        /// The settings for the Cassandra snapshot store.
        /// </summary>
        public CassandraSnapshotStoreConfig SnapshotStoreConfig { get; private set; }
        
        public CassandraExtension(ExtendedActorSystem system)
        {
            if (system == null) throw new ArgumentNullException("system");
            
            // Initialize fallback configuration defaults
            system.Settings.InjectTopLevelFallback(CassandraPersistence.DefaultConfig());

            // Get or add the session manager
            //SessionManager = CassandraSession.Instance.Apply(system);
            
            // Read config
            var journalConfig = system.Settings.Config.GetConfig("cassandra-journal");
            JournalConfig = new CassandraJournalConfig(system, journalConfig);

            var snapshotConfig = system.Settings.Config.GetConfig("cassandra-snapshot-store");
            SnapshotStoreConfig = new CassandraSnapshotStoreConfig(system, snapshotConfig);
        }
    }
}
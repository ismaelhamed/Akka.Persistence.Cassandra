using System.Threading.Tasks;
using Akka.Actor;

namespace Akka.Persistence.Cassandra.Tests
{
    /// <summary>
    /// Some static helper methods for resetting Cassandra between tests or test contexts.
    /// </summary>
    public static class TestSetupHelpers
    {
        public static void ResetJournalData(ActorSystem sys)
        {
            // Get or add the extension
            var ext = CassandraPersistence.Instance.Apply(sys);

            // Use session to remove keyspace
            var session = new CassandraSession(sys, ext.JournalConfig, sys.Log, "", s => Task.FromResult(new object()));
            Await.Result(session.Underlying, 3000).DeleteKeyspaceIfExists(ext.JournalConfig.Keyspace);
            session.Underlying.Dispose();
        }

        public static void ResetSnapshotStoreData(ActorSystem sys)
        {
            // Get or add the extension
            var ext = CassandraPersistence.Instance.Apply(sys);

            // Use session to remove the keyspace
            var session = new CassandraSession(sys, ext.SnapshotStoreConfig, sys.Log, "", s => Task.FromResult(new object()));
            Await.Result(session.Underlying, 3000).DeleteKeyspaceIfExists(ext.SnapshotStoreConfig.Keyspace);
            session.Underlying.Dispose();
        }
    }
}

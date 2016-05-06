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
            var cassandraSession = new CassandraSession(sys, ext.JournalConfig, sys.Log, "",
                s => Task.FromResult(new object()));
            using (var session = Await.Result(cassandraSession.Underlying, 3000))
            {
                session.Execute($"DROP KEYSPACE IF EXISTS {ext.JournalConfig.Keyspace}");
            }
        }

        public static void ResetSnapshotStoreData(ActorSystem sys)
        {
            // Get or add the extension
            var ext = CassandraPersistence.Instance.Apply(sys);

            // Use session to remove the keyspace
            var cassandraSession = new CassandraSession(sys, ext.SnapshotStoreConfig, sys.Log, "",
                s => Task.FromResult(new object()));
            using (var session = Await.Result(cassandraSession.Underlying, 3000))
            {
                session.Execute($"DROP KEYSPACE IF EXISTS {ext.SnapshotStoreConfig.Keyspace}");
            }
        }
    }
}

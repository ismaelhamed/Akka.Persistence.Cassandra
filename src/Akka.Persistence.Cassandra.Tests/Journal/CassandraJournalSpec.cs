using Akka.Configuration;
using Akka.Persistence.TestKit.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class CassandraJournalSpec : JournalSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
cassandra-journal.port = {CassandraConfig.Port}
cassandra-snapshot-store.port = {CassandraConfig.Port}
cassandra-journal.keyspace = CassandraJournalSpec
cassandra-snapshot-store.keyspace = CassandraJournalSpecSnapshot"
                ).WithFallback(CassandraPersistenceSpec.Config);

        public CassandraJournalSpec(ITestOutputHelper output = null) : base(Config, "CassandraJournalSpec", output)
        {
            CassandraPersistenceSpec.BeforeAll(this);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects => false;

        [Fact(Skip = "Metrics not implemented yet")]
        public void A_Cassandra_Journal_must_insert_Cassandra_metrics_to_Cassandra_Metrics_Registry()
        {
            // TODO Metrics
        }
    }
}
using Akka.Configuration;
using Akka.Persistence.TestKit.Journal;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class CassandraJournalCompat2Spec : JournalSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                @"
cassandra-journal.cassandra-2x-compat = on
cassandra-journal.keyspace = CassandraJournalCompat2Spec
cassandra-snapshot-store.keyspace = CassandraJournalCompat2Spec"
                ).WithFallback(CassandraJournalSpec.Config);

        public CassandraJournalCompat2Spec(ITestOutputHelper output = null) : base(Config, "CassandraJournalCompat2Spec", output)
        {
            CassandraPersistenceSpec.BeforeAll(this);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects => false;
    }
}
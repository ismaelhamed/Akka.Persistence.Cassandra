using Akka.Configuration;
using Akka.Persistence.TestKit.Journal;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    // TODO JournalPerfSpec not implemented yet
    /*public class CassandraJournalPerfSpec : JournalPerfSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                @"
cassandra-journal.cassandra-2x-compat = on
cassandra-journal.keyspace = CassandraJournalPerfSpec
cassandra-snapshot-store.keyspace = CassandraJournalPerfSpecSnapshot"
                ).WithFallback(CassandraJournalSpec.Config);

        public CassandraJournalPerfSpec(ITestOutputHelper output = null) : base(Config, "CassandraJournalPerfSpec", output)
        {
            CassandraPersistenceSpec.BeforeAll(this);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects => false;
    }*/
}
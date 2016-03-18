//-----------------------------------------------------------------------
// <copyright file="CassandraJournalProtocolV3Spec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TestKit.Journal;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    // Cassandra 2.2.0 or later should support protocol version V4, but as long as we
    //support 2.1.6+ we do some compatibility testing with V3.
    public class CassandraJournalProtocolV3Spec : JournalSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                @"
cassandra-journal.protocol-version = 3
cassandra-journal.keyspace = CassandraJournalProtocolV3Spec
cassandra-snapshot-store.keyspace = CassandraJournalProtocolV3Spec"
                ).WithFallback(CassandraJournalSpec.Config);

        public CassandraJournalProtocolV3Spec(ITestOutputHelper output = null) : base(Config, "CassandraJournalProtocolV3Spec", output)
        {
            CassandraPersistenceSpec.BeforeAll(this);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects => false;
    }
}
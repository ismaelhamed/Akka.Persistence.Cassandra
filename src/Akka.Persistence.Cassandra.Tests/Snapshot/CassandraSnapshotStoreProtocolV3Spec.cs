//-----------------------------------------------------------------------
// <copyright file="CassandraSnapshotStoreProtocolV3Spec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TestKit.Snapshot;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Snapshot
{
    public class CassandraSnapshotStoreProtocolV3Spec : SnapshotStoreSpec
    {
        private new static readonly Config Config =
            ConfigurationFactory.ParseString(
                @"
cassandra-journal.protocol-version = 3
cassandra-journal.keyspace = SnapshotStoreV3Spec
cassandra-snapshot-store.keyspace = SnapshotStoreV3SpecSnapshot"
                ).WithFallback(CassandraSnapshotStoreSpec.Config);

        public CassandraSnapshotStoreProtocolV3Spec(ITestOutputHelper output = null) : base(Config, "CassandraSnapshotStoreProtocolV3Spec", output)
        {
            CassandraPersistenceSpec.BeforeAll(this);
            Initialize();
        }
    }
}
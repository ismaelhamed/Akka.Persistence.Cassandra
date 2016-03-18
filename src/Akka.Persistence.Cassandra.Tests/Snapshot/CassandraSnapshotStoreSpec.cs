//-----------------------------------------------------------------------
// <copyright file="CassandraSnapshotStoreSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Text;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Snapshot;
using Akka.Persistence.TestKit.Snapshot;
using Cassandra;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Snapshot
{
    public class CassandraSnapshotStoreSpec : SnapshotStoreSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
cassandra-journal.port = {CassandraConfig.Port}
cassandra-snapshot-store.port = {CassandraConfig.Port}
cassandra-journal.keyspace = SnapshotStoreSpec
cassandra-snapshot-store.keyspace = SnapshotStoreSpecSnapshot
cassandra-snapshot-store.max-metadata-result-size = 2"
                ).WithFallback(CassandraPersistenceSpec.Config);

        private readonly CassandraStatements _storeStatements;
        private readonly ISession _session;
        // ByteArraySerializer
        private const int SerializerId = 4;

        public CassandraSnapshotStoreSpec(ITestOutputHelper output = null) : base(Config, "CassandraSnapshotStoreSpec", output)
        {
            CassandraPersistenceSpec.BeforeAll(this);
            var storeConfig = new CassandraSnapshotStoreConfig(Sys, Sys.Settings.Config.GetConfig("cassandra-snapshot-store"));
            _storeStatements = new CassandraStatements(storeConfig);
            _session = Await.Result(storeConfig.SessionProvider.Connect(), 5000);
            Initialize();
        }

        protected override void AfterAll()
        {
            _session.Dispose();
            _session.Cluster.Dispose();
            base.AfterAll();
        }

        [Fact(Skip = "Metrics not implemented yet")]
        public void A_Cassandra_snapshot_store_must_insert_Cassandra_metrics_to_Cassandra_Metrics_Registry()
        {
            // TODO Metrics
        }

        [Fact]
        public void A_Cassandra_snapshot_store_must_make_up_to_3_snapshot_loading_attempts()
        {
            var probe = CreateTestProbe();

            // load most recent snapshot
            SnapshotStore.Tell(new LoadSnapshot(Pid, SnapshotSelectionCriteria.Latest, long.MaxValue), probe.Ref);

            // get most recent snapshot
            var expected = probe.ExpectMsg<LoadSnapshotResult>().Snapshot;

            // write two more snapshots that cannot be de-serialized
            _session.Execute(new SimpleStatement(_storeStatements.WriteSnapshot, Pid, 17L, 123L, SerializerId, "", Encoding.UTF8.GetBytes("fail-1"), null));
            _session.Execute(new SimpleStatement(_storeStatements.WriteSnapshot, Pid, 18L, 124L, SerializerId, "", Encoding.UTF8.GetBytes("fail-2"), null));

            // load most recent snapshot, first two attempts will fail ...
            SnapshotStore.Tell(new LoadSnapshot(Pid, SnapshotSelectionCriteria.Latest, long.MaxValue), probe.Ref);

            // third attempt succeeds
            var result = probe.ExpectMsg<LoadSnapshotResult>();
            result.ToSequenceNr.Should().Be(long.MaxValue);
            result.Snapshot.Should().Be(expected);
        }

        [Fact]
        public void A_Cassandra_snapshot_store_must_give_up_after_3_snapshot_loading_attempts()
        {
            var probe = CreateTestProbe();

            // load most recent snapshot
            SnapshotStore.Tell(new LoadSnapshot(Pid, SnapshotSelectionCriteria.Latest, long.MaxValue), probe.Ref);

            // wait for most recent snapshot
            probe.ExpectMsg<LoadSnapshotResult>();

            // write two more snapshots that cannot be de-serialized
            _session.Execute(new SimpleStatement(_storeStatements.WriteSnapshot, Pid, 17L, 123L, SerializerId, "", Encoding.UTF8.GetBytes("fail-1"), null));
            _session.Execute(new SimpleStatement(_storeStatements.WriteSnapshot, Pid, 18L, 124L, SerializerId, "", Encoding.UTF8.GetBytes("fail-2"), null));
            _session.Execute(new SimpleStatement(_storeStatements.WriteSnapshot, Pid, 19L, 125L, SerializerId, "", Encoding.UTF8.GetBytes("fail-3"), null));

            // load most recent snapshot, first two attempts will fail ...
            SnapshotStore.Tell(new LoadSnapshot(Pid, SnapshotSelectionCriteria.Latest, long.MaxValue), probe.Ref);

            // no 4th attempt has been made
            var result = probe.ExpectMsg<LoadSnapshotResult>();
            result.ToSequenceNr.Should().Be(long.MaxValue);
            result.Snapshot.Should().BeNull();
        }
    }
}
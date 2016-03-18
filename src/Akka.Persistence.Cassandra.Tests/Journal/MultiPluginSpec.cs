//-----------------------------------------------------------------------
// <copyright file="MultiPluginSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Journal;
using Cassandra;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class MultiPluginSpec : CassandraPersistenceSpec
    {
        private const string JournalKeyspace = "multiplugin_spec_journal";
        private const string SnapshotKeyspace = "multiplugin_spec_snapshot";
        private static readonly int CassandraPort = CassandraConfig.Port;

        // hack around lack of proper support for substitutions
        private static readonly Config CustomConfig = ConfigurationFactory.ParseString(
            $@"
cassandra-journal.keyspace={JournalKeyspace}
cassandra-journal.port={CassandraPort}
cassandra-journal.keyspace-autocreate=false
cassandra-journal.circuit-breaker.call-timeout = 30s
cassandra-snapshot-store.keyspace={SnapshotKeyspace}
cassandra-snapshot-store.port={CassandraPort}
cassandra-snapshot-store.keyspace-autocreate=false
");

        private new static readonly Config DefaultConfig =
            ConfigurationFactory.FromResource<CassandraJournal>("Akka.Persistence.Cassandra.reference.conf");

        private static readonly string DefaultJournalConfig =
            CustomConfig.GetConfig("cassandra-journal")
                .WithFallback(DefaultConfig.GetConfig("cassandra-journal"))
                .ToString(true)
                .Replace(":", "=")
                .Replace(" = \r\n", " = \"\"\r\n");

        private static readonly string DefaultSnapshotConfig =
            CustomConfig.GetConfig("cassandra-snapshot-store")
                .WithFallback(DefaultConfig.GetConfig("cassandra-snapshot-store"))
                .ToString(true)
                .Replace(":", "=")
                .Replace(" = \r\n", " = \"\"\r\n");

        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                @"
akka.test.single-expect-default = 20s

cassandra-journal-a={{cassandra-journal}}
cassandra-journal-a.table=processor_a_messages

cassandra-journal-b={{cassandra-journal}}
cassandra-journal-b.table=processor_b_messages

cassandra-journal-c={{cassandra-journal}}
cassandra-journal-c.table=processor_c_messages
cassandra-snapshot-c={{cassandra-snapshot-store}}
cassandra-snapshot-c.table=snapshot_c_messages

cassandra-journal-d={{cassandra-journal}}
cassandra-journal-d.table=processor_d_messages
cassandra-snapshot-d={{cassandra-snapshot-store}}
cassandra-snapshot-d.table=snapshot_d_messages"
                    .Replace("{{cassandra-journal}}", DefaultJournalConfig)
                    .Replace("{{cassandra-snapshot-store}}", DefaultSnapshotConfig)
                );

        private abstract class Processor : PersistentActor
        {
            protected override bool ReceiveRecover(object message)
            {
                return true;
            }

            protected override bool ReceiveCommand(object message)
            {
                if (message is SaveSnapshotSuccess)
                {
                }
                else if (message.Equals("snapshot"))
                {
                    SaveSnapshot("snapshot");
                }
                else
                {
                    Persist(message, e => { Sender.Tell($"{message}-{LastSequenceNr}", Self); });
                }
                return false;
            }
        }

        private class OverrideJournalPluginProcessor : Processor
        {
            public OverrideJournalPluginProcessor(string journalPluginId)
            {
                JournalPluginId = journalPluginId;
                SnapshotPluginId = "cassandra-snapshot-store";
            }

            public override string PersistenceId => "always-the-same";
        }

        private class OverrideSnapshotPluginProcessor : Processor
        {
            public OverrideSnapshotPluginProcessor(string journalPluginId, string snapshotPluginId)
            {
                JournalPluginId = journalPluginId;
                SnapshotPluginId = snapshotPluginId;
            }

            public override string PersistenceId => "always-the-same";
        }

        private readonly ISession _session;

        public MultiPluginSpec(ITestOutputHelper output = null) : base(Config, "MultiPluginSpec", output)
        {
            var cassandraPluginConfig = new CassandraPluginConfig(Sys,
                Sys.Settings.Config.GetConfig("cassandra-journal"));
            _session = Await.Result(cassandraPluginConfig.SessionProvider.Connect(), TimeSpan.FromSeconds(25));

            _session.Execute(
                $"DROP KEYSPACE IF EXISTS {JournalKeyspace}");
            _session.Execute(
                $"DROP KEYSPACE IF EXISTS {SnapshotKeyspace}");

            _session.Execute(
                $"CREATE KEYSPACE IF NOT EXISTS {JournalKeyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}");
            _session.Execute(
                $"CREATE KEYSPACE IF NOT EXISTS {SnapshotKeyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}");
        }

        // default journal plugin is not configured for this test
        protected override void AwaitPersistenceInit()
        {
        }

        protected override void AfterAll()
        {
            _session.Dispose();
            _session.Cluster.Dispose();
            base.AfterAll();
        }

        [Fact]
        public void
            A_Cassandra_journal_should_be_usable_multiple_times_with_different_configurations_for_two_actors_having_the_same_persistence_id
            ()
        {
            var processorA = Sys.ActorOf(Props.Create(() => new OverrideJournalPluginProcessor("cassandra-journal-a")));
            processorA.Tell("msg");
            ExpectMsg("msg-1");

            var processorB = Sys.ActorOf(Props.Create(() => new OverrideJournalPluginProcessor("cassandra-journal-b")));
            processorB.Tell("msg");
            ExpectMsg("msg-1");

            processorB.Tell("msg");
            ExpectMsg("msg-2");

            // c is actually a and therefore the next message must be seqNr 2 and not 3
            var processorC = Sys.ActorOf(Props.Create(() => new OverrideJournalPluginProcessor("cassandra-journal-a")));
            processorC.Tell("msg");
            ExpectMsg("msg-2");
        }

        [Fact]
        public void
            A_Cassandra_snapshot_store_should_be_usable_multiple_times_with_different_configurations_for_two_actors_having_the_same_persistence_id
            ()
        {

            var processorC =
                Sys.ActorOf(
                    Props.Create(
                        () => new OverrideSnapshotPluginProcessor("cassandra-journal-c", "cassandra-snapshot-c")));
            processorC.Tell("msg");
            ExpectMsg("msg-1");

            var processorD =
                Sys.ActorOf(
                    Props.Create(
                        () => new OverrideSnapshotPluginProcessor("cassandra-journal-d", "cassandra-snapshot-d")));
            processorD.Tell("msg");
            ExpectMsg("msg-1");

            processorD.Tell("msg");
            ExpectMsg("msg-2");

            processorC.Tell("snapshot");
            processorD.Tell("snapshot");

            // e is actually c and therefore the next message must be seqNr 2 after recovery by using the snapshot
            var processorE =
                Sys.ActorOf(
                    Props.Create(
                        () => new OverrideSnapshotPluginProcessor("cassandra-journal-c", "cassandra-snapshot-c")));
            processorE.Tell("msg");
            ExpectMsg("msg-2");

            // e is actually d and therefore the next message must be seqNr 3 after recovery by using the snapshot
            var processorF =
                Sys.ActorOf(
                    Props.Create(
                        () => new OverrideSnapshotPluginProcessor("cassandra-journal-d", "cassandra-snapshot-d")));
            processorF.Tell("msg");
            ExpectMsg("msg-3");
        }
    }
}
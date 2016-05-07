using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class CassandraIntegrationSpec : CassandraPersistenceSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
akka.persistence.journal.max-deletion-batch-size = 3
akka.persistence.publish-confirmations = on
akka.persistence.publish-plugin-commands = on
cassandra-journal.target-partition-size = 5
cassandra-journal.max-result-size = 3
cassandra-journal.port = {CassandraConfig.Port}
cassandra-snapshot-store.port = {CassandraConfig.Port}
cassandra-journal.keyspace = CassandraIntegrationSpec
cassandra-snapshot-store.keyspace = CassandraIntegrationSpecSnapshot"
                ).WithFallback(CassandraPersistenceSpec.Config);

        private sealed class DeleteTo
        {
            public DeleteTo(long sequenceNr)
            {
                SequenceNr = sequenceNr;
            }

            public long SequenceNr { get; }
        }

        private class ProcessorAtomic : PersistentActor
        {
            private readonly IActorRef _receiver;

            public ProcessorAtomic(string persistenceId, IActorRef receiver)
            {
                _receiver = receiver;
                PersistenceId = persistenceId;
            }

            public override string PersistenceId { get; }

            protected override bool ReceiveRecover(object message)
            {
                return Handle(message);
            }

            protected override bool ReceiveCommand(object message)
            {
                if (message is DeleteTo)
                {
                    DeleteMessages(((DeleteTo) message).SequenceNr);
                }
                else if (message is IList)
                {
                    PersistAll(((IList) message).Cast<object>(), e => Handle(e));
                }
                else return false;
                return true;
            }

            private bool Handle(object message)
            {
                if (message is string)
                {
                    _receiver.Tell(message);
                    _receiver.Tell(LastSequenceNr);
                    _receiver.Tell(IsRecovering);
                    return true;
                }
                return false;
            }
        }

        private class ProcessorA : PersistentActor
        {
            private readonly IActorRef _receiver;

            public ProcessorA(string persistenceId, IActorRef receiver)
            {
                _receiver = receiver;
                PersistenceId = persistenceId;
            }

            public override string PersistenceId { get; }

            protected override bool ReceiveRecover(object message)
            {
                return Handle(message);
            }

            protected override bool ReceiveCommand(object message)
            {
                if (message is DeleteTo)
                {
                    DeleteMessages(((DeleteTo) message).SequenceNr);
                }
                else if (message is string)
                {
                    Persist(message, e => Handle(e));
                }
                else return false;
                return true;
            }

            private bool Handle(object message)
            {
                if (message is string)
                {
                    _receiver.Tell(message);
                    _receiver.Tell(LastSequenceNr);
                    _receiver.Tell(IsRecovering);
                    return true;
                }
                return false;
            }
        }

        private class ProcessorC : PersistentActor
        {
            private readonly IActorRef _probe;
            private string _last;

            public ProcessorC(string persistenceId, IActorRef probe)
            {
                _probe = probe;
                PersistenceId = persistenceId;
            }

            public override string PersistenceId { get; }

            protected override bool ReceiveRecover(object message)
            {
                if ((message as SnapshotOffer)?.Snapshot is string)
                {
                    _last = (string) ((SnapshotOffer) message).Snapshot;
                    _probe.Tell($"offered-{_last}");
                }
                else if (message is string)
                {
                    return Handle(message);
                }
                else return false;
                return true;
            }

            protected override bool ReceiveCommand(object message)
            {
                if (message.Equals("snap"))
                {
                    SaveSnapshot(_last);
                }
                else if (message is SaveSnapshotSuccess)
                {
                    _probe.Tell($"snapped-{_last}");
                }
                else if (message is string)
                {
                    Persist(message, e => Handle(e));
                }
                else if (message is DeleteTo)
                {
                    DeleteMessages(((DeleteTo) message).SequenceNr);
                }
                else return false;
                return true;
            }

            private bool Handle(object message)
            {
                if (message is string)
                {
                    _last = $"{message}-{LastSequenceNr}";
                    _probe.Tell($"updated-{_last}");
                    return true;
                }
                return false;
            }
        }

        private class ProcessorCNoRecover : ProcessorC
        {
            public ProcessorCNoRecover(string persistenceId, IActorRef probe, Recovery recoverConfig) : base(persistenceId, probe)
            {
                Recovery = recoverConfig;
            }

            public override Recovery Recovery { get; }

            protected override void PreStart()
            {
            }
        }

        private class ViewA : PersistentView
        {
            private readonly IActorRef _probe;

            public ViewA(string viewId, string persistenceId, IActorRef probe)
            {
                _probe = probe;
                ViewId = viewId;
                PersistenceId = persistenceId;
            }

            public override string ViewId { get; }
            public override string PersistenceId { get; }

            public override bool IsAutoUpdate => false;
            public override long AutoUpdateReplayMax => 0;

            protected override bool Receive(object message)
            {
                _probe.Tell(message);
                return true;
            }
        }

        public CassandraIntegrationSpec(ITestOutputHelper output = null) : base(Config, "CassandraIntegrationSpec", output)
        {
        }

        [Fact]
        public void A_Cassandra_journal_should_write_and_replay_messages()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)), "p1");
            for (var i = 1L; i <= 16L; i++)
            {
                processor1.Tell($"a-{i}");
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }

            var processor2 = Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)), "p2");
            for (var i = 1L; i <= 16L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, true);
            }

            processor2.Tell("b");
            ExpectMsgAllOf<object>("b", 17L, false);
        }

        [Fact]
        public void A_Cassandra_journal_should_not_reply_range_deleted_messages()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var deleteProbe = CreateTestProbe();
            SubscribeToRangeDeletion(deleteProbe);

            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)));
            for (var i = 1L; i <= 16L; i++)
            {
                processor1.Tell($"a-{i}");
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }

            processor1.Tell(new DeleteTo(3));
            AwaitRangeDeletion(deleteProbe);

            Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)));
            for (var i = 4L; i <= 16L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, true);
            }

            processor1.Tell(new DeleteTo(7));
            AwaitRangeDeletion(deleteProbe);

            Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)));
            for (var i = 8L; i <= 16L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, true);
            }
        }

        [Fact]
        public void A_Cassandra_journal_should_replay_messages_incrementally()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var probe = CreateTestProbe();

            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)));
            for (var i = 1L; i <= 6L; i++)
            {
                processor1.Tell($"a-{i}");
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }

            var view = Sys.ActorOf(Props.Create(() => new ViewA("p7-view", persistenceId, probe.Ref)));
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

            view.Tell(new Update(true, 3L));
            probe.ExpectMsg("a-1");
            probe.ExpectMsg("a-2");
            probe.ExpectMsg("a-3");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

            view.Tell(new Update(true, 3L));
            probe.ExpectMsg("a-4");
            probe.ExpectMsg("a-5");
            probe.ExpectMsg("a-6");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));
        }

        [Fact]
        public void A_Cassandra_journal_should_write_and_replay_with_PersistAll_greater_than_partition_size_skipping_whole_partition()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processorAtomic = Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, TestActor)));

            processorAtomic.Tell(new List<string> {"a-1", "a-2", "a-3", "a-4", "a-5", "a-6"});
            for (var i = 1L; i <= 6L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }

            var testProbe = CreateTestProbe();
            Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, testProbe.Ref)));
            for (var i = 1L; i <= 6L; i++)
            {
                testProbe.ExpectMsgAllOf<object>($"a-{i}", i, true);
            }
        }

        [Fact]
        public void A_Cassandra_journal_should_write_and_replay_with_PersistAll_greater_than_partition_size_skipping_part_of_a_partition()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processorAtomic = Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, TestActor)));

            processorAtomic.Tell(new List<string> {"a-1", "a-2", "a-3"});
            for (var i = 1L; i <= 3L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }

            processorAtomic.Tell(new List<string> {"a-4", "a-5", "a-6"});
            for (var i = 4L; i <= 6L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }

            var testProbe = CreateTestProbe();
            Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, testProbe.Ref)));
            for (var i = 1L; i <= 6L; i++)
            {
                testProbe.ExpectMsgAllOf<object>($"a-{i}", i, true);
            }
        }

        [Fact]
        public void A_Cassandra_journal_should_write_and_replay_with_PersistAll_less_than_partition_size()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processorAtomic = Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, TestActor)));

            processorAtomic.Tell(new List<string> {"a-1", "a-2", "a-3", "a-4"});
            for (var i = 1L; i <= 4L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }

            var testProbe = CreateTestProbe();
            Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, testProbe.Ref)));
            for (var i = 1L; i <= 4L; i++)
            {
                testProbe.ExpectMsgAllOf<object>($"a-{i}", i, true);
            }
        }

        [Fact]
        public void A_Cassandra_journal_should_not_replay_messages_deleted_from_the_plus_1_partition()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var deleteProbe = CreateTestProbe();
            SubscribeToRangeDeletion(deleteProbe);
            var processorAtomic = Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, TestActor)));

            processorAtomic.Tell(new List<string> {"a-1", "a-2", "a-3", "a-4", "a-5", "a-6"});
            for (var i = 1L; i <= 6L; i++)
            {
                ExpectMsgAllOf<object>($"a-{i}", i, false);
            }
            processorAtomic.Tell(new DeleteTo(5L));
            AwaitRangeDeletion(deleteProbe);

            var testProbe = CreateTestProbe();
            Sys.ActorOf(Props.Create(() => new ProcessorAtomic(persistenceId, testProbe.Ref)));
            testProbe.ExpectMsgAllOf<object>("a-6", 6L, true);
        }

        [Fact]
        public void A_processor_should_recover_from_a_snapshot_with_follow_up_messages()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));

            processor1.Tell("a");
            ExpectMsg("updated-a-1");
            processor1.Tell("snap");
            ExpectMsg("snapped-a-1");
            processor1.Tell("b");
            ExpectMsg("updated-b-2");

            Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));
            ExpectMsg("offered-a-1");
            ExpectMsg("updated-b-2");
        }

        [Fact]
        public void A_processor_should_recover_from_a_snapshot_with_follow_up_messages_and_an_upper_bound()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorCNoRecover(persistenceId, TestActor, new Recovery())));

            processor1.Tell("a");
            ExpectMsg("updated-a-1");
            processor1.Tell("snap");
            ExpectMsg("snapped-a-1");
            for (var i = 2L; i <= 7L; i++)
            {
                processor1.Tell("a");
                ExpectMsg($"updated-a-{i}");
            }

            var processor2 = Sys.ActorOf(Props.Create(() => new ProcessorCNoRecover(persistenceId, TestActor, new Recovery(null, 3L))));
            ExpectMsg("offered-a-1");
            ExpectMsg("updated-a-2");
            ExpectMsg("updated-a-3");
            processor2.Tell("d");
            ExpectMsg("updated-d-8");
        }

        [Fact]
        public void A_processor_should_recover_from_a_snapshot_without_follow_up_messages_inside_a_partition()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));

            processor1.Tell("a");
            ExpectMsg("updated-a-1");
            processor1.Tell("snap");
            ExpectMsg("snapped-a-1");

            var processor2 = Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));
            ExpectMsg("offered-a-1");
            processor2.Tell("b");
            ExpectMsg("updated-b-2");
        }

        [Fact]
        public void A_processor_should_recover_from_a_snapshot_without_follow_up_messages_at_a_partition_boundary_where_next_partition_is_invalid()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));
            for (var i = 1L; i <= 5L; i++)
            {
                processor1.Tell("a");
                ExpectMsg($"updated-a-{i}");
            }
            processor1.Tell("snap");
            ExpectMsg("snapped-a-5");

            var processor2 = Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));
            ExpectMsg("offered-a-5");
            processor2.Tell("b");
            ExpectMsg("updated-b-6");
        }

        [Fact]
        public void A_processor_should_recover_from_a_snapshot_without_follow_up_messages_at_a_partition_boundary_where_next_partition_contains_a_permanently_deleted_message()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var deleteProbe = CreateTestProbe();
            SubscribeToRangeDeletion(deleteProbe);

            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));
            for (var i = 1L; i <= 5L; i++)
            {
                processor1.Tell("a");
                ExpectMsg($"updated-a-{i}");
            }
            processor1.Tell("snap");
            ExpectMsg("snapped-a-5");

            processor1.Tell("a");
            ExpectMsg("updated-a-6");

            processor1.Tell(new DeleteTo(6L));
            AwaitRangeDeletion(deleteProbe);

            var processor2 = Sys.ActorOf(Props.Create(() => new ProcessorC(persistenceId, TestActor)));
            ExpectMsg("offered-a-5");
            processor2.Tell("b");
            ExpectMsg("updated-b-7"); // no longer re-using sequence numbers
        }

        [Fact]
        public void A_processor_should_properly_recover_after_all_messages_have_been_deleted()
        {
            var persistenceId = Guid.NewGuid().ToString();
            var deleteProbe = CreateTestProbe();
            SubscribeToRangeDeletion(deleteProbe);

            var p = Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)));
            p.Tell("a");
            ExpectMsgAllOf<object>("a", 1L, false);

            p.Tell(new DeleteTo(1L));
            AwaitRangeDeletion(deleteProbe);

            var r = Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceId, TestActor)));
            r.Tell("b");
            ExpectMsgAllOf<object>("b", 2L, false); // no longer re-using sequence numbers
        }

        private void SubscribeToRangeDeletion(TestProbe probe)
        {
            Sys.EventStream.Subscribe(probe.Ref, typeof(DeleteMessagesTo));
        }

        private static void AwaitRangeDeletion(TestProbe probe)
        {
            probe.ExpectMsg<DeleteMessagesTo>();
        }
    }
}
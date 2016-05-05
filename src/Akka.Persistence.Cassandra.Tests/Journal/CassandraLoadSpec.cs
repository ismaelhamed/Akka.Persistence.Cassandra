using System;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Util.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class CassandraLoadSpec : CassandraPersistenceSpec
    {
        private new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
cassandra-journal.port = {CassandraConfig.Port}
cassandra-snapshot-store.port = {CassandraConfig.Port}
cassandra-journal.keyspace = CassandraLoadSpec
cassandra-snapshot-store.keyspace = CassandraLoadSpecSnapshot
akka.actor.serialize-messages = off"
                ).WithFallback(CassandraPersistenceSpec.Config);

        private class ProcessorA : PersistentActor
        {
            private long _startTime;
            private long _stopTime;

            private long _startSequenceNr;
            private long _stopSequenceNr;

            public ProcessorA(string persistenceId)
            {
                PersistenceId = persistenceId;
            }

            public override string PersistenceId { get; }

            protected override bool ReceiveRecover(object message)
            {
                return Handle(message);
            }

            protected override bool ReceiveCommand(object message)
            {
                if (message.Equals("start"))
                    DeferAsync(message, e =>
                    {
                        StartMeasure();
                        Sender.Tell("started");
                    });
                else if (message.Equals("stop"))
                    DeferAsync(message, e => { StopMeasure(); });
                else if (message is string)
                    PersistAsync(message, e => Handle(e));
                else return false;
                return true;
            }

            private static bool Handle(object message)
            {
                return message is string;
            }

            private void StartMeasure()
            {
                _startSequenceNr = LastSequenceNr;
                _startTime = DateTime.UtcNow.Ticks;
            }

            private void StopMeasure()
            {
                _stopSequenceNr = LastSequenceNr;
                _stopTime = DateTime.UtcNow.Ticks;
                Sender.Tell(((double) TimeSpan.TicksPerSecond) * (_stopSequenceNr - _startSequenceNr) / (_stopTime - _startTime));
            }
        }

        private class ProcessorB : PersistentActor
        {
            private readonly long? _failAt;
            private readonly IActorRef _receiver;

            public ProcessorB(string persistenceId, long? failAt, IActorRef receiver)
            {
                PersistenceId = persistenceId;
                _failAt = failAt;
                _receiver = receiver;
            }

            public override string PersistenceId { get; }

            protected override bool ReceiveRecover(object message)
            {
                return Handle(message);
            }

            protected override bool ReceiveCommand(object message)
            {
                if (message is string)
                {
                    PersistAsync(message, e => Handle(e));
                    return true;
                }
                return false;
            }

            private bool Handle(object message)
            {
                if (message is string)
                {
                    _receiver.Tell($"{message}-{LastSequenceNr}");
                    return true;
                }
                return false;
            }
        }

        private readonly ITestOutputHelper _output;

        public CassandraLoadSpec(ITestOutputHelper output = null) : base(Config, "CassandraLoadSpec", output)
        {
            _output = output;
        }

        protected override string SystemName => "CassandraLoadSpec";

        [Fact]
        public void A_Cassandra_journal_should_have_some_reasonable_write_throughput()
        {
            const int warmCycles = 100;
            const int loadCycles = 1000;

            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorA("p1a")));
            Enumerable.Range(1, warmCycles).ForEach(i => processor1.Tell("a"));
            processor1.Tell("start");
            ExpectMsg("started");
            Enumerable.Range(1, loadCycles).ForEach(i => processor1.Tell("a"));
            processor1.Tell("stop");
            ExpectMsg<double>(m =>
            {
                _output.WriteLine($"throughput = {m} persistent commands per second");
            }, TimeSpan.FromSeconds(100));
        }

        [Fact]
        public void A_Cassandra_journal_should_work_properly_under_load()
        {
            const int cycles = 1000;

            var processor1 = Sys.ActorOf(Props.Create(() => new ProcessorB("p1b", null, TestActor)));
            Enumerable.Range(1, cycles).ForEach(i => processor1.Tell("a"));
            Enumerable.Range(1, cycles).ForEach(i => ExpectMsg($"a-{i}"));

            var processor2 = Sys.ActorOf(Props.Create(() => new ProcessorB("p1b", null, TestActor)));
            Enumerable.Range(1, cycles).ForEach(i => ExpectMsg($"a-{i}"));

            processor2.Tell("b");
            ExpectMsg($"b-{cycles + 1}");
        }
    }
}
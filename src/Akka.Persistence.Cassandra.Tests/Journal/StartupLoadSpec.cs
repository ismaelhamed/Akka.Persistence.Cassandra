using System;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class StartupLoadSpec : CassandraPersistenceSpec
    {
        private new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
cassandra-journal.port = {CassandraConfig.Port}
cassandra-snapshot-store.port = {CassandraConfig.Port}
cassandra-journal.keyspace = StartupLoadSpec
cassandra-snapshot-store.keyspace = StartupLoadSpecSnapshot"
                ).WithFallback(CassandraPersistenceSpec.Config);

        private class ProcessorA : PersistentActor
        {
            private readonly IActorRef _receiver;

            public ProcessorA(string persistenceId, IActorRef receiver)
            {
                PersistenceId = persistenceId;
                _receiver = receiver;
            }

            public override string PersistenceId { get; }

            protected override bool ReceiveRecover(object message)
            {
                return true;
            }

            protected override bool ReceiveCommand(object message)
            {
                if (message is string)
                {
                    var payload = (string) message;
                    Persist(payload, Handle);
                    return true;
                }
                return false;
            }

            private void Handle(string payload)
            {
                _receiver.Tell(payload);
                _receiver.Tell(LastSequenceNr);
                SaveSnapshot(payload);
            }
        }

        public StartupLoadSpec(ITestOutputHelper output = null) : base(Config, "StartupLoadSpec", output)
        {
        }

        protected override void AwaitPersistenceInit()
        {
            // important, since we are testing the initialization
        }

        [Fact]
        public void Journal_initialization_should_handle_many_persistent_actors_starting_at_the_same_time()
        {
            const int N = 500;
            for (var i = 1; i <= 3; i++)
            {
                var probes = Enumerable.Range(1, N).Select(n =>
                {
                    var probe = CreateTestProbe();
                    var persistenceid = n.ToString();
                    var r = Sys.ActorOf(Props.Create(() => new ProcessorA(persistenceid, probe.Ref)));
                    r.Tell($"a-{i}");
                    return probe;
                }).ToList();

                probes.ForEach(p =>
                {
                    if (i == 1 && p == probes[0])
                        p.ExpectMsg($"a-{i}", TimeSpan.FromSeconds(30));
                    else
                        p.ExpectMsg($"a-{i}");
                    p.ExpectMsg((long) i);
                });
            }
        }
    }
}
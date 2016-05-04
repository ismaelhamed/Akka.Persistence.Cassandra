using System;
using Akka.Actor;
using Akka.Configuration;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests
{
    public abstract class CassandraPersistenceSpec : Akka.TestKit.Xunit2.TestKit
    {
        public static readonly Config Config = ConfigurationFactory.ParseString(@"
akka.persistence.journal.plugin = ""cassandra-journal""
akka.persistence.snapshot-store.plugin = ""cassandra-snapshot-store""
cassandra-journal.circuit-breaker.call-timeout = 30s
akka.test.single-expect-default = 20s
akka.actor.serialize-messages = off
        ");

        internal class AwaitPersistenceInitActor : PersistentActor
        {
            public override string PersistenceId => "persistenceInit";


            protected override bool ReceiveRecover(object message)
            {
                return true;
            }

            protected override bool ReceiveCommand(object message)
            {
                Persist(message, _ =>
                {
                    Sender.Tell(message);
                    Context.Stop(Self);
                });
                return true;
            }
        }

        protected abstract string SystemName { get; }

        protected CassandraPersistenceSpec(Config config, string actorSystemName = null, ITestOutputHelper output = null) : base(config, actorSystemName, output)
        {
            TestSetupHelpers.ResetJournalData(Sys);
            TestSetupHelpers.ResetSnapshotStoreData(Sys);
            AwaitPersistenceInit(Sys);
        }

        protected void AwaitPersistenceInit()
        {
            AwaitPersistenceInit(Sys);
        }

        private void AwaitPersistenceInit(ActorSystem system)
        {
            var probe = CreateTestProbe(system);
            system.ActorOf(Props.Create(() => new AwaitPersistenceInitActor())).Tell("hello", probe.Ref);
            probe.ExpectMsg("hello", TimeSpan.FromSeconds(35));
        }
    }
}
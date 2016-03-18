//-----------------------------------------------------------------------
// <copyright file="CassandraPersistenceSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
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
akka.actor.serialize-messages = on
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

        protected CassandraPersistenceSpec(Config config, string actorSystemName = null, ITestOutputHelper output = null) : base(config, actorSystemName, output)
        {
            TestSetup.ResetJournalData(Sys);
            TestSetup.ResetSnapshotStoreData(Sys);
            AwaitPersistenceInit();
        }

        protected virtual void AwaitPersistenceInit()
        {
            AwaitPersistenceInit(Sys);
        }

        private void AwaitPersistenceInit(ActorSystem system)
        {
            AwaitPersistenceInit(CreateTestProbe(system));
        }

        public static void BeforeAll(TestKitBase test)
        {
            TestSetup.ResetJournalData(test.Sys);
            TestSetup.ResetSnapshotStoreData(test.Sys);
            AwaitPersistenceInit(test.CreateTestProbe());
        }

        private static void AwaitPersistenceInit(TestProbe probe)
        {
            var t0 = DateTime.UtcNow;
            var n = 0;
            probe.Within(TimeSpan.FromSeconds(45), () =>
            {
                probe.AwaitAssert(() =>
                {
                    n += 1;
                    probe.Sys.ActorOf(Props.Create(() => new AwaitPersistenceInitActor()), $"persistenceInit{n}").Tell("hello", probe.Ref);
                    probe.ExpectMsg("hello", TimeSpan.FromSeconds(5));
                    probe.Sys.Log.Debug($"awaitPersistenceInit took {(DateTime.UtcNow - t0).TotalMilliseconds}ms {probe.Sys.Name}");
                });
            });
        }
    }
}
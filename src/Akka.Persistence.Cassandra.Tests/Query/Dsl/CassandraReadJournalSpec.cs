//-----------------------------------------------------------------------
// <copyright file="CassandraReadJournalSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Query;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Query.Dsl
{
    public class CassandraReadJournalSpec : CassandraPersistenceSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
akka.loglevel = INFO
cassandra-journal.port = {CassandraConfig.Port}
cassandra-journal.keyspace=JavadslCassandraReadJournalSpec
cassandra-query-journal.max-buffer-size = 10
cassandra-query-journal.refresh-interval = 0.5s
cassandra-query-journal.eventual-consistency-delay = 1s
cassandra-journal.event-adapters {{
    test-tagger = ""Akka.Persistence.Cassandra.Tests.Query.Dsl.CassandraReadJournalSpec+TestTagger, Akka.Persistence.Cassandra.Tests""
}}
cassandra-journal.event-adapter-bindings = {{
    ""System.String"" = test-tagger
}}")
                .WithFallback(CassandraPersistenceSpec.Config);

        public class TestTagger : IWriteEventAdapter
        {
            public string Manifest(object evt)
            {
                return "";
            }

            public object ToJournal(object evt)
            {
                return ((string) evt).StartsWith("a") ? new Tagged(evt, new[] {"a"}) : evt;
            }
        }

        private readonly ActorMaterializer _materializer;
        private readonly CassandraReadJournal _queries;

        public CassandraReadJournalSpec(ITestOutputHelper output = null) : base(Config, "CassandraReadJournalSpec", output)
        {
            _materializer = ActorMaterializer.Create(Sys);
            _queries = PersistenceQuery.Get(Sys).ReadJournalFor<CassandraReadJournal>(CassandraReadJournal.Identifier);
        }

        [Fact]
        public void Cassandra_Read_Journal_API_must_start_EventsByPersistenceId_query()
        {
            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            a.Tell("a-1", TestActor);
            ExpectMsg("a-1-done");

            var src = _queries.EventsByPersistenceId("a", 0L, long.MaxValue);
            var probe = src.Map(env => env.PersistenceId).RunWith(this.SinkProbe<string>(), _materializer);
            probe
                .Request(10)
                .ExpectNext("a");
            probe
                .Cancel();
        }

        [Fact]
        public void Cassandra_Read_Journal_API_must_start_CurrentEventsByPersistenceId_query()
        {
            var b = Sys.ActorOf(Query.TestActor.Props("b"));
            b.Tell("b-1", TestActor);
            ExpectMsg("b-1-done");

            var src = _queries.CurrentEventsByPersistenceId("b", 0L, long.MaxValue);
            var probe = src.Map(env => env.PersistenceId).RunWith(this.SinkProbe<string>(), _materializer);
            probe
                .Request(10)
                .ExpectNext("b");
            probe
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_Read_Journal_API_must_start_EventsByTag_query()
        {
            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            a.Tell("a-1", TestActor);
            ExpectMsg("a-1-done");
            var b = Sys.ActorOf(Query.TestActor.Props("b"));
            b.Tell("b-1", TestActor);
            ExpectMsg("b-1-done");

            var src = _queries.EventsByTag("a", 0L);
            var probe = src.Map(env => env.PersistenceId).RunWith(this.SinkProbe<string>(), _materializer);
            probe
                .Request(10)
                .ExpectNext("a")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe
                .Cancel();
        }

        [Fact]
        public void Cassandra_Read_Journal_API_must_start_CurrentEventsByTag_query()
        {
            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            a.Tell("a-1", TestActor);
            ExpectMsg("a-1-done");
            var b = Sys.ActorOf(Query.TestActor.Props("b"));
            b.Tell("b-1", TestActor);
            ExpectMsg("b-1-done");

            var src = _queries.CurrentEventsByTag("a", 0L);
            var probe = src.Map(env => env.PersistenceId).RunWith(this.SinkProbe<string>(), _materializer);
            probe
                .Request(10)
                .ExpectNext("a");
            probe
                .ExpectComplete();
        }
    }
}
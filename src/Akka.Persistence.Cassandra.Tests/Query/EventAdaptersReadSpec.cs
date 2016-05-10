//-----------------------------------------------------------------------
// <copyright file="EventAdaptersReadSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Journal;
using Akka.Persistence.Cassandra.Query;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class EventAdaptersReadSpec : CassandraPersistenceSpec
    {
        private static readonly DateTime Today = DateTime.UtcNow.Date;

        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
    akka.loglevel = INFO
    cassandra-journal.port = {CassandraConfig.Port}
    cassandra-journal.keyspace=EventAdaptersReadSpec
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 2
    cassandra-journal.target-partition-size = 15
    cassandra-journal.event-adapters.test = ""Akka.Persistence.Cassandra.Tests.Query.TestEventAdapter, Akka.Persistence.Cassandra.Tests""
    cassandra-journal.event-adapter-bindings {{
      ""System.String"" = test
    }}
    cassandra-journal.tags {{
      red = 1
      yellow = 1
      green = 1
    }}
    cassandra-query-journal {{
      refresh-interval = 500ms
      max-buffer-size = 50
      first-time-bucket = {new TimeBucket
                    (Today.AddDays(-5)).Key}
      eventual-consistency-delay = 2s
    }}")
                .WithFallback(CassandraPersistenceSpec.Config);

        private readonly ActorMaterializer _materializer;
        private readonly CassandraReadJournal _queries;

        public EventAdaptersReadSpec(ITestOutputHelper output = null) : base(Config, "EventAdaptersReadSpec", output)
        {
            _materializer = ActorMaterializer.Create(Sys);
            _queries = PersistenceQuery.Get(Sys).ReadJournalFor<CassandraReadJournal>(CassandraReadJournal.Identifier);
        }

        private void Setup(string persistenceId, int n, Func<int, string> prefix = null)
        {
            var @ref = Sys.ActorOf(Query.TestActor.Props(persistenceId));
            for (var i = 1; i <= n; i++)
            {
                var message = $"{(prefix != null ? prefix(i) : "")}{persistenceId}-{i}";
                @ref.Tell(message);
                ExpectMsg($"{message}-done");
            }
        }

        private static Func<int, string> Tagged(string tag, Func<int, string> f = null)
        {
            return i => $"tagged:{tag}:{(f != null ? f(i) : "")}";
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_not_replay_dropped_events_by_the_event_adapter()
        {
            Setup("a", 6, i => i%2 == 0 ? "dropped:" : "");

            var src = _queries.CurrentEventsByPersistenceId("a", 0L, long.MaxValue);
            var probe = src.Map(e => (string) e.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(2)
                .ExpectNext("a-1", "a-3")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(500));
            probe.Request(2)
                .ExpectNext("a-5")
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_replay_duplicate_events_by_the_event_adapter()
        {
            Setup("b", 3, i => i%2 == 0 ? "duplicated:" : "");

            var src = _queries.CurrentEventsByPersistenceId("b", 0L, long.MaxValue);
            var probe = src.Map(e => (string) e.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(10)
                .ExpectNext("b-1", "b-2", "b-2", "b-3")
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_duplicate_events_with_prefix_added_by_the_event_adapter()
        {
            Setup("c", 1, _ => "prefixed:foo:");

            var src = _queries.CurrentEventsByPersistenceId("c", 0L, long.MaxValue);
            var probe = src.Map(e => (string) e.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(10)
                .ExpectNext("foo-c-1")
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByTag_must_not_replay_events_dropped_by_the_event_adapter()
        {
            Setup("d", 6, Tagged("red", i => i%2 == 0 ? "dropped:" : ""));

            var src = _queries.CurrentEventsByTag("red", 0L);
            var probe = src.Map(e => (string) e.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(10)
                .ExpectNext("d-1", "d-3", "d-5")
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByTag_must_replay_events_duplicated_by_the_event_adapter()
        {
            Setup("e", 3, Tagged("yellow", i => i%2 == 0 ? "duplicated:" : ""));

            var src = _queries.CurrentEventsByTag("yellow", 0L);
            var probe = src.Map(e => (string) e.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(10)
                .ExpectNext("e-1", "e-2", "e-2", "e-3")
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByTag_must_replay_events_transformed_by_the_event_adapter()
        {
            Setup("f", 3, Tagged("green", i => i%2 == 0 ? "prefixed:foo:" : ""));

            var src = _queries.CurrentEventsByTag("green", 0L);
            var probe = src.Map(e => (string) e.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(10)
                .ExpectNext("f-1", "foo-f-2", "f-3")
                .ExpectComplete();
        }
    }
}
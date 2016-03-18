//-----------------------------------------------------------------------
// <copyright file="EventsByPersistenceIdSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Query;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class EventsByPersistenceIdSpec : CassandraPersistenceSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
    akka.loglevel = INFO
    cassandra-journal.port = {CassandraConfig.Port}
    cassandra-journal.keyspace=EventsByPersistenceIdSpec
    cassandra-query-journal.max-buffer-size = 10
    cassandra-query-journal.refresh-interval = 0.5s
    cassandra-query-journal.max-result-size-query = 2
    cassandra-journal.target-partition-size = 15")
                .WithFallback(CassandraPersistenceSpec.Config);

        private readonly ActorMaterializer _materializer;
        private readonly CassandraReadJournal _queries;

        public EventsByPersistenceIdSpec(ITestOutputHelper output = null) : base(Config, "EventsByPersistenceIdSpec", output)
        {
            _materializer = ActorMaterializer.Create(Sys);
            _queries = PersistenceQuery.Get(Sys).ReadJournalFor<CassandraReadJournal>(CassandraReadJournal.Identifier);
        }

        private IActorRef Setup(string persistenceId, int n)
        {
            var @ref = Sys.ActorOf(Query.TestActor.Props(persistenceId));
            for (var i = 1; i <= n; i++)
            {
                @ref.Tell($"{persistenceId}-{i}");
                ExpectMsg($"{persistenceId}-{i}-done");
            }

            return @ref;
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_find_existing_events()
        {
            Setup("a", 3);

            var src = _queries.CurrentEventsByPersistenceId("a", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);
                probe.Request(2)
                .ExpectNext("a-1", "a-2")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(500));
            probe.Request(2)
                .ExpectNext("a-3")
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_find_existing_events_from_a_sequence_number()
        {
            Setup("b", 10);

            var src = _queries.CurrentEventsByPersistenceId("b", 5L, long.MaxValue);
            src.Select(env => env.SequenceNr).RunWith(this.SinkProbe<long>(), _materializer)
                .Request(6)
                .ExpectNext(5L, 6L, 7L, 8L, 9L, 10L)
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_not_see_any_events_if_the_streams_starts_after_current_latest_event()
        {
            Setup("c", 3);

            var src = _queries.CurrentEventsByPersistenceId("c", 5L, long.MaxValue);
            src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer)
                .Request(5)
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_find_existing_events_up_to_a_sequence_number()
        {
            Setup("d", 3);

            var src = _queries.CurrentEventsByPersistenceId("d", 0L, 2L);
            src.Select(env => env.SequenceNr).RunWith(this.SinkProbe<long>(), _materializer)
                .Request(5)
                .ExpectNext(1L, 2L)
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_not_see_new_events_after_demand_request()
        {
            var @ref = Setup("e", 3);

            var src = _queries.CurrentEventsByPersistenceId("e", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(2)
                .ExpectNext("e-1", "e-2")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            @ref.Tell("e-4");
            ExpectMsg("e-4-done");

            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe.Request(5)
                .ExpectNext("e-3")
                .ExpectComplete(); // e-4 not seen
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_only_deliver_what_requested_if_there_is_more_in_the_buffer()
        {
            Setup("f", 1000);

            var src = _queries.CurrentEventsByPersistenceId("f", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(2)
                .ExpectNext("f-1", "f-2")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(1000));
            probe.Request(5)
                .ExpectNext("f-3", "f-4", "f-5", "f-6", "f-7")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            probe.Request(5)
                .ExpectNext("f-8", "f-9", "f-10", "f-11", "f-12")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_stop_if_there_are_no_events()
        {
            var src = _queries.CurrentEventsByPersistenceId("g", 0L, long.MaxValue);

            src.RunWith(this.SinkProbe<EventEnvelope>(), _materializer)
                .Request(2)
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_produce_correct_sequence_of_sequence_numbers_and_offsets()
        {
            Setup("h", 3);

            var src = _queries.CurrentEventsByPersistenceId("h", 0L, long.MaxValue);
            src.Select(env => Tuple.Create(env.PersistenceId, env.SequenceNr, env.Offset))
                .RunWith(this.SinkProbe<Tuple<string, long, long>>(), _materializer)
                .Request(3)
                .ExpectNext(Tuple.Create("h", 1L, 1L), Tuple.Create("h", 2L, 2L), Tuple.Create("h", 3L, 3L))
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_EventsByPersistenceId_must_produce_correct_sequence_of_events_across_multiple_partitions()
        {
            Setup("i", 20);

            var src = _queries.CurrentEventsByPersistenceId("i", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(10)
                .ExpectNextN(Enumerable.Range(1, 10).Select(i => $"i-{i}"))
                .ExpectNoMsg(TimeSpan.FromMilliseconds(500));

            probe
                .Request(10)
                .ExpectNextN(Enumerable.Range(11, 10).Select(i => $"i-{i}"))
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_find_new_events()
        {
            var @ref = Setup("j", 3);

            var src = _queries.EventsByPersistenceId("j", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(5)
                .ExpectNext("j-1", "j-2", "j-3");

            @ref.Tell("j-4");
            ExpectMsg("j-4-done");

            probe
                .ExpectNext("j-4");
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_find_new_events_if_the_stream_starts_after_current_latest_event()
        {
            var @ref = Setup("k", 4);

            var src = _queries.EventsByPersistenceId("k", 5L, long.MaxValue);
            var probe = src.Select(env => env.SequenceNr).RunWith(this.SinkProbe<long>(), _materializer);

            probe
                .Request(5)
                .ExpectNoMsg(TimeSpan.FromMilliseconds(500));

            @ref.Tell("k-5");
            ExpectMsg("k-5-done");
            @ref.Tell("k-6");
            ExpectMsg("k-6-done");

            probe.ExpectNext(5L, 6L);

            @ref.Tell("k-7");
            ExpectMsg("k-7-done");

            probe.ExpectNext(7L);
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_find_new_events_up_to_a_sequence_nr()
        {
            var @ref = Setup("l", 3);

            var src = _queries.EventsByPersistenceId("l", 0L, 4L);
            var probe = src.Select(env => env.SequenceNr).RunWith(this.SinkProbe<long>(), _materializer);

            probe
                .Request(5)
                .ExpectNext(1L, 2L, 3L);

            @ref.Tell("l-4");
            ExpectMsg("l-4-done");

            probe
                .ExpectNext(4L)
                .ExpectComplete();
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_find_new_events_after_demand_request()
        {
            var @ref = Setup("m", 3);

            var src = _queries.EventsByPersistenceId("m", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(2)
                .ExpectNext("m-1", "m-2");

            @ref.Tell("m-4");
            ExpectMsg("m-4-done");

            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            probe
                .Request(5)
                .ExpectNext("m-3")
                .ExpectNext("m-4");
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_only_deliver_what_requested_if_there_is_more_in_the_buffer()
        {
            Setup("n", 1000);

            var src = _queries.EventsByPersistenceId("n", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(2)
                .ExpectNext("n-1", "n-2")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            probe
                .Request(5)
                .ExpectNext("n-3", "n-4", "n-5", "n-6", "n-7")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            probe
                .Request(5)
                .ExpectNext("n-8", "n-9", "n-10", "n-11", "n-12")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_not_produce_anything_if_there_arent_any_events()
        {
            Setup("o2", 1); // Database init.

            var src = _queries.EventsByPersistenceId("o", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(10)
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_not_produce_anything_until_there_are_existing_events()
        {
            Setup("p2", 1); // Database init.

            var src = _queries.EventsByPersistenceId("p", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(2)
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            Setup("p", 2);

            probe
                .ExpectNext("p-1", "p-2")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));
        }

        [Fact]
        public void Cassandra_live_query_EventsByPersistenceId_must_produce_correct_sequence_of_events_across_multiple_partitions()
        {
            var @ref = Setup("q", 15);

            var src = _queries.EventsByPersistenceId("q", 0L, long.MaxValue);
            var probe = src.Select(env => (string) env.Event).RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(16)
                .ExpectNextN(Enumerable.Range(1, 15).Select(i => $"q-{i}"))
                .ExpectNoMsg(TimeSpan.FromMilliseconds(500));

            for (var i = 16L; i <= 21L; i++)
            {
                @ref.Tell($"q-{i}");
                ExpectMsg($"q-{i}-done");
            }

            probe
                .Request(6)
                .ExpectNextN(Enumerable.Range(16, 6).Select(i => $"q-{i}"));

            for (var i = 22L; i <= 35L; i++)
            {
                @ref.Tell($"q-{i}");
                ExpectMsg($"q-{i}-done");
            }

            probe
                .Request(10)
                .ExpectNextN(Enumerable.Range(22, 10).Select(i => $"q-{i}"));
        }
    }
}
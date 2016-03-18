//-----------------------------------------------------------------------
// <copyright file="EventsByTagSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.Configuration;
using Akka.Pattern;
using Akka.Persistence.Cassandra.Journal;
using Akka.Persistence.Cassandra.Query;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.TestKit;
using Cassandra;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class ColorFruitTagger : IWriteEventAdapter
    {
        private readonly HashSet<string> _colors = new HashSet<string> {"green", "black", "blue", "yellow"};
        private readonly HashSet<string> _fruits = new HashSet<string> {"apple", "banana"};

        public object ToJournal(object @event)
        {
            if (@event is string)
            {
                var s = (string) @event;
                var colorTags = _colors.Where(c => s.Contains(c));
                var fruitTags = _fruits.Where(c => s.Contains(c));
                var tags = colorTags.Union(fruitTags).ToList();
                return tags.Count == 0 ? @event : new Tagged(@event, tags);

            }
            return @event;
        }

        public string Manifest(object @event) => "";
    }

    public abstract class AbstractEventsByTagSpec : CassandraPersistenceSpec
    {
        protected readonly ActorMaterializer Materializer;
        protected readonly Lazy<CassandraReadJournal> Queries;
        private readonly Akka.Serialization.Serialization _serialization;
        private readonly CassandraJournalConfig _writePluginConfig;
        private readonly Lazy<ISession> _session;
        private readonly Lazy<PreparedStatement> _preparedWriteMessage;

        protected AbstractEventsByTagSpec(Config config, string actorSystemName = null, ITestOutputHelper output = null) : base(config, actorSystemName, output)
        {
            Materializer = ActorMaterializer.Create(Sys);
            Queries =
                new Lazy<CassandraReadJournal>(
                    () =>
                        PersistenceQuery.Get(Sys).ReadJournalFor<CassandraReadJournal>(CassandraReadJournal.Identifier));
            _serialization = Sys.Serialization;
            _writePluginConfig = new CassandraJournalConfig(Sys, Sys.Settings.Config.GetConfig("cassandra-journal"));
            _session = new Lazy<ISession>(() => Await.Result(_writePluginConfig.SessionProvider.Connect(), TimeSpan.FromSeconds(5000)));
            _preparedWriteMessage =
                new Lazy<PreparedStatement>(
                    () => _session.Value.Prepare(new CassandraStatements(_writePluginConfig).WriteMessage));
        }

        protected void WriteTestEvent(DateTime time, Persistent persistent, HashSet<string> tags)
        {
            var serialized = _serialization.FindSerializerFor(persistent).ToBinary(persistent);

            var tagsById = GetTagsById(tags);
            var parameters = new
            {
                persistence_id = persistent.PersistenceId,
                partition_nr = 1L,
                sequence_nr = persistent.SequenceNr,
                timestamp = TimeUuid.NewId(time),
                timebucket = new TimeBucket(time).Key,
                tag1 = GetTagById(tagsById, 1),
                tag2 = GetTagById(tagsById, 2),
                tag3 = GetTagById(tagsById, 3),
                message = serialized
            };
            var boundStatement = _preparedWriteMessage.Value.Bind(parameters);
            _session.Value.Execute(boundStatement);
        }

        private IDictionary<int, string> GetTagsById(IEnumerable<string> tags)
        {
            var tagsById = new Dictionary<int, string>();
            foreach (var tag in tags)
            {
                int tagId;
                if (!_writePluginConfig.Tags.TryGetValue(tag, out tagId)) tagId = 1;

                tagsById[tagId] = tag;
            }
            return tagsById;
        }

        private static string GetTagById(IDictionary<int, string> tagsById, int tagId)
        {
            string tag;
            return tagsById.TryGetValue(tagId, out tag) ? tag : null;
        }

        protected static EventEnvelope ExpectEvent(TestSubscriber.ManualProbe<EventEnvelope> probe, string persistenceId,
            long? sequenceNr, string @event)
        {
            var envelope = probe.ExpectNext();
            envelope.PersistenceId.Should().Be(persistenceId);
            if (sequenceNr.HasValue)
                envelope.SequenceNr.Should().Be(sequenceNr.Value);
            envelope.Event.Should().Be(@event);
            return envelope;
        }

        protected static GuidEventEnvelope ExpectEvent(TestSubscriber.ManualProbe<GuidEventEnvelope> probe, string persistenceId,
            long? sequenceNr, string @event)
        {
            var envelope = probe.ExpectNext();
            envelope.PersistenceId.Should().Be(persistenceId);
            if (sequenceNr.HasValue)
                envelope.SequenceNr.Should().Be(sequenceNr.Value);
            envelope.Event.Should().Be(@event);
            return envelope;
        }

        protected override void AfterAll()
        {
            if (_session.IsValueCreated)
            {
                _session.Value.Dispose();
                _session.Value.Cluster.Dispose();
            }
            base.AfterAll();
        }
    }

    public class EventsByTagSpec : AbstractEventsByTagSpec
    {
        private static readonly DateTime Today = DateTime.UtcNow.Date;

        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
akka.loglevel = INFO
cassandra-journal {{
  #target-partition-size = 5
  port = {CassandraConfig
                    .Port}
  keyspace=EventsByTagSpec
  event-adapters {{
    color-tagger  = ""Akka.Persistence.Cassandra.Tests.Query.ColorFruitTagger, Akka.Persistence.Cassandra.Tests""
  }}
  event-adapter-bindings = {{
    ""System.String"" = color-tagger
  }}
  tags {{
    green = 1
    black = 1
    blue = 1
    pink = 1
    yellow = 1
    apple = 2
    banana = 2
    #T1 = 1
    T2 = 2
    T3 = 3
    #T4 = 1
  }}
}}
cassandra-query-journal {{
  refresh-interval = 500ms
  max-buffer-size = 50
  first-time-bucket = {new TimeBucket
                        (Today.AddDays(-5)).Key}
  eventual-consistency-delay = 2s
}}")
                .WithFallback(CassandraPersistenceSpec.Config);

        public EventsByTagSpec(ITestOutputHelper output = null) : base(Config, "EventsByTagSpec", output)
        {
        }

        private void Setup()
        {
            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            var b = Sys.ActorOf(Query.TestActor.Props("b"));

            a.Tell("hello", TestActor);
            ExpectMsg("hello-done", TimeSpan.FromSeconds(20));
            a.Tell("a green apple", TestActor);
            ExpectMsg("a green apple-done");
            b.Tell("a black car", TestActor);
            ExpectMsg("a black car-done");
            a.Tell("something else", TestActor);
            ExpectMsg("something else-done");
            a.Tell("a green banana", TestActor);
            ExpectMsg("a green banana-done");
            b.Tell("a green leaf", TestActor);
            ExpectMsg("a green leaf-done");
        }

        [Fact]
        public void Cassandra_query_CurrentEventsByTag_must_implement_standard_ICurrentEventsByTagQuery()
        {
            // ReSharper disable once IsExpressionAlwaysTrue
            (Queries.Value is ICurrentEventsByTagQuery).Should().BeTrue();
        }

        [Fact]
        public void Cassandra_query_CurrentEventsByTag_must_find_existing_events()
        {
            Setup();

            var greenSrc = Queries.Value.CurrentEventsByTag("green", 0L);
            var probe = greenSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            ExpectEvent(probe, "a", 2L, "a green apple");
            ExpectEvent(probe, "a", 4L, "a green banana");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
            probe.Request(2);
            ExpectEvent(probe, "b", 2L, "a green leaf");
            probe.ExpectComplete();

            var blackSrc = Queries.Value.CurrentEventsByTag("black", 0L);
            var probe2 = blackSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe2.Request(5);
            ExpectEvent(probe2, "b", 1L, "a black car");

            probe2.ExpectComplete();

            var appleSrc = Queries.Value.CurrentEventsByTag("apple", 0L);
            var probe3 = appleSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe3.Request(5);
            ExpectEvent(probe3, "a", 2L, "a green apple");
            probe3.ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_CurrentEventsByTag_must_complete_when_no_events()
        {
            var src = Queries.Value.CurrentEventsByTag("pink", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            probe.ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_CurrentEventsByTag_must_not_see_new_events_after_demand_request()
        {
            Setup();

            var c = Sys.ActorOf(Query.TestActor.Props("c"));

            var greenSrc = Queries.Value.CurrentEventsByTag("green", 0L);
            var probe = greenSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            ExpectEvent(probe, "a", 2L, "a green apple");
            ExpectEvent(probe, "a", 4L, "a green banana");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            c.Tell("a green cucumber", TestActor);
            ExpectMsg("a green cucumber-done");

            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe.Request(5);
            ExpectEvent(probe, "b", 2L, "a green leaf");
            probe.ExpectComplete(); // green cucumber not seen
        }

        [Fact]
        public void Cassandra_query_CurrentEventsByTag_must_find_events_from_timestamp_offset()
        {
            Setup();

            var greenSrc1 = Queries.Value.CurrentEventsByTag("green", 0L);
            var probe1 = greenSrc1.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe1.Request(2);
            ExpectEvent(probe1, "a", 2L, "a green apple");
            var offset = ExpectEvent(probe1, "a", 4L, "a green banana").Offset;
            probe1.Cancel();

            var greenSrc2 = Queries.Value.CurrentEventsByTag("green", offset);
            var probe2 = greenSrc2.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe2.Request(10);
            ExpectEvent(probe2, "a", 4L, "a green banana");
            ExpectEvent(probe2, "b", 2L, "a green leaf");
            probe2.Cancel();
        }

        [Fact]
        public void Cassandra_query_CurrentEventsByTag_must_find_events_from_Guid_offset()
        {
            Setup();

            var greenSrc1 = Queries.Value.CurrentEventsByTag("green", Queries.Value.FirstOffset);
            var probe1 = greenSrc1.RunWith(this.SinkProbe<GuidEventEnvelope>(), Materializer);
            probe1.Request(2);
            ExpectEvent(probe1, "a", 2L, "a green apple");
            var offset = ExpectEvent(probe1, "a", 4L, "a green banana").Offset;
            probe1.Cancel();

            var greenSrc2 = Queries.Value.CurrentEventsByTag("green", offset);
            var probe2 = greenSrc2.RunWith(this.SinkProbe<GuidEventEnvelope>(), Materializer);
            probe2.Request(10);
            ExpectEvent(probe2, "b", 2L, "a green leaf");
            probe2.Cancel();
        }

        [Fact]
        public void Cassandra_query_CurrentEventsByTag_must_find_events_that_span_several_time_buckets()
        {
            var t1 = Today.AddDays(-5).Date.AddHours(13);
            var w1 = Guid.NewGuid().ToString();
            var pr1 = new Persistent("e1", 1L, "p1", "", writerGuid: w1);
            WriteTestEvent(t1, pr1, new HashSet<string> {"T1"});
            var t2 = t1.AddHours(1);
            var pr2 = new Persistent("e2", 2L, "p1", "", writerGuid: w1);
            WriteTestEvent(t2, pr2, new HashSet<string> {"T1"});
            var t3 = t1.AddDays(1);
            var pr3 = new Persistent("e3", 3L, "p1", "", writerGuid: w1);
            WriteTestEvent(t3, pr3, new HashSet<string> {"T1"});
            var t4 = t1.AddDays(3);
            var pr4 = new Persistent("e4", 4L, "p1", "", writerGuid: w1);
            WriteTestEvent(t4, pr4, new HashSet<string> {"T1"});

            var src = Queries.Value.CurrentEventsByTag("T1", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            ExpectEvent(probe, "p1", 1L, "e1");
            ExpectEvent(probe, "p1", 2L, "e2");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
            probe.Request(5);
            ExpectEvent(probe, "p1", 3L, "e3");
            ExpectEvent(probe, "p1", 4L, "e4");
            probe.ExpectComplete();
        }

        [Fact]
        public void Cassandra_live_EventsByTag_must_implement_standard_IEventsByTagQuery()
        {
            // ReSharper disable once IsExpressionAlwaysTrue
            (Queries.Value is IEventsByTagQuery).Should().BeTrue();
        }

        [Fact]
        public void Cassandra_live_EventsByTag_must_find_new_events()
        {
            Setup();

            var d = Sys.ActorOf(Query.TestActor.Props("d"));

            var blackSrc = Queries.Value.EventsByTag("black", 0L);
            var probe = blackSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(2);
            ExpectEvent(probe, "b", 1L, "a black car");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            d.Tell("a black dog", TestActor);
            ExpectMsg("a black dog-done");
            d.Tell("a black night", TestActor);
            ExpectMsg("a black night-done");

            ExpectEvent(probe, "d", 1L, "a black dog");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe.Request(10);
            ExpectEvent(probe, "d", 2L, "a black night");
            probe.Cancel();
        }

        [Fact]
        public void Cassandra_live_EventsByTag_must_find_events_from_timestamp_offset()
        {
            Setup();

            var greenSrc1 = Queries.Value.EventsByTag("green", 0L);
            var probe1 = greenSrc1.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe1.Request(2);
            ExpectEvent(probe1, "a", 2L, "a green apple");
            var offset = ExpectEvent(probe1, "a", 4L, "a green banana").Offset;
            probe1.Cancel();

            var greenSrc2 = Queries.Value.EventsByTag("green", offset);
            var probe2 = greenSrc2.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe2.Request(10);
            ExpectEvent(probe2, "a", 4L, "a green banana");
            ExpectEvent(probe2, "b", 2L, "a green leaf");
            probe2.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe2.Cancel();
        }

        [Fact]
        public void Cassandra_live_EventsByTag_must_find_events_from_Guid_offset()
        {
            Setup();

            var greenSrc1 = Queries.Value.EventsByTag("green", Queries.Value.FirstOffset);
            var probe1 = greenSrc1.RunWith(this.SinkProbe<GuidEventEnvelope>(), Materializer);
            probe1.Request(2);
            ExpectEvent(probe1, "a", 2L, "a green apple");
            var offset = ExpectEvent(probe1, "a", 4L, "a green banana").Offset;
            probe1.Cancel();

            var greenSrc2 = Queries.Value.EventsByTag("green", offset);
            var probe2 = greenSrc2.RunWith(this.SinkProbe<GuidEventEnvelope>(), Materializer);
            probe2.Request(10);
            ExpectEvent(probe2, "b", 2L, "a green leaf");
            probe2.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            probe2.Cancel();
        }

        [Fact]
        public void Cassandra_live_EventsByTag_must_find_events_that_span_several_time_buckets()
        {
            var t1 = Today.AddDays(-5).Date.AddHours(13);
            var w1 = Guid.NewGuid().ToString();
            var pr1 = new Persistent("e1", 1L, "p1", "", writerGuid: w1);
            WriteTestEvent(t1, pr1, new HashSet<string> {"T1"});
            var t2 = t1.AddHours(1);
            var pr2 = new Persistent("e2", 2L, "p1", "", writerGuid: w1);
            WriteTestEvent(t2, pr2, new HashSet<string> {"T1"});

            var src = Queries.Value.EventsByTag("T1", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(10);
            ExpectEvent(probe, "p1", 1L, "e1");
            ExpectEvent(probe, "p1", 2L, "e2");

            var t3 = DateTime.UtcNow.AddMinutes(-5);
            var pr3 = new Persistent("e3", 3L, "p1", "", writerGuid: w1);
            WriteTestEvent(t3, pr3, new HashSet<string> {"T1"});
            var t4 = DateTime.UtcNow;
            var pr4 = new Persistent("e4", 4L, "p1", "", writerGuid: w1);
            WriteTestEvent(t4, pr4, new HashSet<string> {"T1"});

            ExpectEvent(probe, "p1", 3L, "e3");
            ExpectEvent(probe, "p1", 4L, "e4");
            probe.Cancel();
        }

        [Fact]
        public void Cassandra_live_EventsByTag_must_sort_events_by_timestamp()
        {
            var t1 = DateTime.UtcNow.AddSeconds(-10);
            var w1 = Guid.NewGuid().ToString();
            var w2 = Guid.NewGuid().ToString();

            var pr1 = new Persistent("p1-e1", 1L, "p1", "", writerGuid: w1);
            WriteTestEvent(t1, pr1, new HashSet<string> {"T2"});
            var t3 = DateTime.UtcNow;
            var pr3 = new Persistent("p1-e2", 2L, "p1", "", writerGuid: w1);
            WriteTestEvent(t3, pr3, new HashSet<string> {"T2"});

            var src = Queries.Value.EventsByTag("T2", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(10);

            // simulate async eventually consistent Materialized View update
            // that cause p1-e2 to show up before p2-e1
            Thread.Sleep(500);

            var t2 = t3.AddMilliseconds(-1);
            var pr2 = new Persistent("p2-e1", 1L, "p2", "", writerGuid: w2);
            WriteTestEvent(t2, pr2, new HashSet<string> {"T2"});

            ExpectEvent(probe, "p1", 1L, "p1-e1");
            ExpectEvent(probe, "p2", 1L, "p2-e1");
            ExpectEvent(probe, "p1", 2L, "p1-e2");
            probe.Cancel();
        }

        [Fact]
        public void Cassandra_live_EventsByTag_must_stream_many_events()
        {
            var e = Sys.ActorOf(Query.TestActor.Props("e"));

            var src = Queries.Value.EventsByTag("yellow", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);

            for (var n = 1; n <= 100; n++)
                e.Tell($"yellow-{n}", ActorRefs.NoSender);

            probe.Request(200);
            for (var n = 1; n <= 100; n++)
                ExpectEvent(probe, "e", null, $"yellow-{n}");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            for (var n = 101; n <= 200; n++)
                e.Tell($"yellow-{n}", ActorRefs.NoSender);

            for (var n = 101; n <= 200; n++)
                ExpectEvent(probe, "e", null, $"yellow-{n}");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            probe.Request(10);
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        }
    }

    public class EventsByTagStrictBySeqNoSpec : AbstractEventsByTagSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                @"
cassandra-query-journal.delayed-event-timeout = 3s
").WithFallback(EventsByTagSpec.Config);

        public EventsByTagStrictBySeqNoSpec(ITestOutputHelper output = null) : base(Config, "EventsByTagStrictBySeqNoSpec", output)
        {
        }

        [Fact]
        public void
            Cassandra_live_EventsByTag_with_delayed_event_timeout_greater_than_0_must_detect_missing_sequence_number_and_wait_for_it
            ()
        {
            var t1 = DateTime.UtcNow.AddMinutes(-5);
            var w1 = Guid.NewGuid().ToString();
            var pr1 = new Persistent("e1", 1L, "p1", "", writerGuid: w1);
            WriteTestEvent(t1, pr1, new HashSet<string> {"T3"});

            var t2 = t1.AddSeconds(1);
            var pr2 = new Persistent("e2", 2L, "p1", "", writerGuid: w1);
            WriteTestEvent(t2, pr2, new HashSet<string> {"T3"});

            var t4 = t1.AddSeconds(3);
            var pr4 = new Persistent("e4", 4L, "p1", "", writerGuid: w1);
            WriteTestEvent(t4, pr4, new HashSet<string> {"T3"});

            var src = Queries.Value.EventsByTag("T3", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(10);
            ExpectEvent(probe, "p1", 1L, "e1");
            ExpectEvent(probe, "p1", 2L, "e2");
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));

            var t3 = t1.AddSeconds(2);
            var pr3 = new Persistent("e3", 3L, "p1", "", writerGuid: w1);
            WriteTestEvent(t3, pr3, new HashSet<string> {"T3"});

            ExpectEvent(probe, "p1", 3L, "e3");
            ExpectEvent(probe, "p1", 4L, "e4");
            probe.Cancel();
        }

        [Fact]
        public void
            Cassandra_live_EventsByTag_with_delayed_event_timeout_greater_than_0_must_detect_missing_sequence_number_and_fail_after_timeout
            ()
        {
            var t1 = DateTime.UtcNow.AddMinutes(-5);
            var w1 = Guid.NewGuid().ToString();
            var pr1 = new Persistent("e1", 1L, "p1", "", writerGuid: w1);
            WriteTestEvent(t1, pr1, new HashSet<string> {"T4"});

            var t2 = t1.AddSeconds(1);
            var pr2 = new Persistent("e2", 2L, "p1", "", writerGuid: w1);
            WriteTestEvent(t2, pr2, new HashSet<string> {"T4"});

            var t4 = t1.AddSeconds(3);
            var pr4 = new Persistent("e4", 4L, "p1", "", writerGuid: w1);
            WriteTestEvent(t4, pr4, new HashSet<string> {"T4"});

            var src = Queries.Value.EventsByTag("T4", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(10);
            ExpectEvent(probe, "p1", 1L, "e1");
            ExpectEvent(probe, "p1", 2L, "e2");
            probe.ExpectNoMsg(TimeSpan.FromSeconds(1));
            probe.ExpectError().Should().BeOfType<IllegalStateException>();
        }

        [Fact]
        public void
            Cassandra_live_EventsByTag_with_delayed_event_timeout_greater_than_0_must_detect_missing_sequence_number_and_go_back_to_find_it
            ()
        {
            var t1 = DateTime.UtcNow.AddMinutes(-5);
            var w1 = Guid.NewGuid().ToString();
            var w2 = Guid.NewGuid().ToString();
            var pr1 = new Persistent("p1-e1", 1L, "p1", "", writerGuid: w1);
            WriteTestEvent(t1, pr1, new HashSet<string> {"T5"});

            var t2 = t1.AddSeconds(1);
            var pr2 = new Persistent("p1-e2", 2L, "p1", "", writerGuid: w1);
            WriteTestEvent(t2, pr2, new HashSet<string> {"T5"});

            var t3 = t1.AddSeconds(2);
            var pr3 = new Persistent("p2-e1", 1L, "p2", "", writerGuid: w2);
            WriteTestEvent(t3, pr3, new HashSet<string> {"T5"});

            var t4 = t1.AddSeconds(4);
            var pr4 = new Persistent("p2-e2", 2L, "p2", "", writerGuid: w2);
            WriteTestEvent(t4, pr4, new HashSet<string> {"T5"});

            var src = Queries.Value.EventsByTag("T5", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(10);
            ExpectEvent(probe, "p1", 1L, "p1-e1");
            ExpectEvent(probe, "p1", 2L, "p1-e2");
            ExpectEvent(probe, "p2", 1L, "p2-e1");
            ExpectEvent(probe, "p2", 2L, "p2-e2");

            // too early p1-e4
            var t5 = t1.AddSeconds(5);
            var pr5 = new Persistent("p1-e4", 4L, "p1", "", writerGuid: w1);
            WriteTestEvent(t5, pr5, new HashSet<string> {"T5"});
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));

            // the delayed p1-e3, and the timeuuid is before p2-e2
            var t6 = t1.AddSeconds(3);
            var pr6 = new Persistent("p1-e3", 3L, "p1", "", writerGuid: w1);
            WriteTestEvent(t6, pr6, new HashSet<string> {"T5"});

            ExpectEvent(probe, "p1", 3L, "p1-e3");
            ExpectEvent(probe, "p1", 4L, "p1-e4");

            var t7 = t1.AddSeconds(7);
            var pr7 = new Persistent("p2-e3", 3L, "p2", "", writerGuid: w1);
            WriteTestEvent(t7, pr7, new HashSet<string> {"T5"});

            ExpectEvent(probe, "p2", 3L, "p2-e3");

            probe.Cancel();
        }

        [Fact]
        public void
            Cassandra_live_EventsByTag_with_delayed_event_timeout_greater_than_0_must_find_delayed_events
            ()
        {
            var t1 = DateTime.UtcNow.AddMinutes(-5);
            var w1 = Guid.NewGuid().ToString();
            var w2 = Guid.NewGuid().ToString();
            var pr1 = new Persistent("p1-e1", 1L, "p1", "", writerGuid: w1);
            WriteTestEvent(t1, pr1, new HashSet<string> {"T6"});

            var t2 = t1.AddSeconds(2);
            var pr2 = new Persistent("p2-e1", 1L, "p2", "", writerGuid: w1);
            WriteTestEvent(t2, pr2, new HashSet<string> {"T6"});

            var src = Queries.Value.EventsByTag("T6", 0L);
            var probe = src.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe.Request(10);
            ExpectEvent(probe, "p1", 1L, "p1-e1");
            ExpectEvent(probe, "p2", 1L, "p2-e1");

            // delayed, and timestamp is before p2-e1
            var t3 = t1.AddSeconds(1);
            var pr3 = new Persistent("p1-e2", 2L, "p1", "", writerGuid: w2);
            WriteTestEvent(t3, pr3, new HashSet<string> {"T6"});

            ExpectEvent(probe, "p1", 2L, "p1-e2");

            probe.Cancel();
        }
    }
}
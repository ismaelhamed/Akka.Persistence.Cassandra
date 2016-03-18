//-----------------------------------------------------------------------
// <copyright file="EventsByTagPubsubSpec.cs" company="Akka.NET Project">
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
using Akka.Streams.TestKit;
using Cassandra;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class EventsByTagPubsubSpec : CassandraPersistenceSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                @"
akka.actor.serialize-messages = off
akka.actor.provider = ""Akka.Cluster.ClusterActorRefProvider, Akka.Cluster""
cassandra-journal {{
  pubsub-minimum-interval = 1ms
}}
cassandra-query-journal {{
  refresh-interval = 10s
  eventual-consistency-delay = 0s
}}")
                .WithFallback(EventsByTagSpec.Config);

        private readonly ActorMaterializer _materializer;
        private readonly Lazy<CassandraReadJournal> _queries;
        private readonly Lazy<ISession> _session;

        public EventsByTagPubsubSpec(ITestOutputHelper output = null) : base(Config, "EventsByTagPubsubSpec", output)
        {
            _materializer = ActorMaterializer.Create(Sys);
            _queries = new Lazy<CassandraReadJournal>(() => PersistenceQuery.Get(Sys).ReadJournalFor<CassandraReadJournal>(CassandraReadJournal.Identifier));

            var writePluginConfig = new CassandraJournalConfig(Sys, Sys.Settings.Config.GetConfig("cassandra-journal"));
            _session = new Lazy<ISession>(() => Await.Result(writePluginConfig.SessionProvider.Connect(), TimeSpan.FromSeconds(5)));
            Cluster.Cluster.Get(Sys).Join(Cluster.Cluster.Get(Sys).SelfAddress);
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

        [Fact]
        public void
            Cassandra_query_EventsByTag_when_running_clustered_with_pubsub_enabled_must_present_new_events_to_an_ongoing_EventsByTag_stream_long_before_polling_would_kick_in
            ()
        {
            var actor = Sys.ActorOf(Query.TestActor.Props("EventsByTagPubsubSpec_a"));

            var blackSrc = _queries.Value.EventsByTag("black", 0L);
            var probe = blackSrc.RunWith(this.SinkProbe<EventEnvelope>(), _materializer);
            probe.Request(2);
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(300));

            actor.Tell("a black car", ActorRefs.NoSender);
            probe.Within(TimeSpan.FromSeconds(5), () =>
            {
                // long before refresh-interval, which is 10s
                return probe.ExpectNext<EventEnvelope>(env => env.Event.Equals("a black car"));
            });
        }
    }
}
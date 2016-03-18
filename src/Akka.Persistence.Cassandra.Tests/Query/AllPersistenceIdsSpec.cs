//-----------------------------------------------------------------------
// <copyright file="AllPersistenceIdsSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Query;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Cassandra;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class AllPersistenceIdsSpec : CassandraPersistenceSpec
    {
        public new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
akka.loglevel = INFO
cassandra-journal.port = {CassandraConfig.Port}
cassandra-journal.keyspace=AllPersistenceIdsSpec
cassandra-query-journal.max-buffer-size = 10
cassandra-query-journal.refresh-interval = 0.5s
cassandra-query-journal.max-result-size-query = 10
cassandra-journal.target-partition-size = 15"
                ).WithFallback(CassandraPersistenceSpec.Config);

        private readonly CassandraPluginConfig _pluginConfig;
        private readonly ISession _session;
        private readonly CassandraReadJournal _queries;
        private readonly ActorMaterializer _materializer;

        public AllPersistenceIdsSpec(ITestOutputHelper output = null) : base(Config, "AllPersistenceIdsSpec", output)
        {
            var cfg = Config
                .WithFallback(Sys.Settings.Config)
                .GetConfig("cassandra-journal");
            _pluginConfig = new CassandraPluginConfig(Sys, cfg);
            _session = Await.Result(_pluginConfig.SessionProvider.Connect(), TimeSpan.FromSeconds(5));
            DeleteAllEvents();

            _queries = PersistenceQuery.Get(Sys).ReadJournalFor<CassandraReadJournal>(CassandraReadJournal.Identifier);
            _materializer = ActorMaterializer.Create(Sys);
        }

        protected override void AfterAll()
        {
            _session.Dispose();
            base.AfterAll();
        }

        private void DeleteAllEvents()
        {
            _session.Execute($"TRUNCATE {_pluginConfig.Keyspace}.{_pluginConfig.Table}");
        }

        private Source<string, NotUsed> All()
        {
            return _queries.AllPersistenceIds().WhereNot(e => e == "persistenceInit");
        }

        private Source<string, NotUsed> Current()
        {
            return _queries.CurrentPersistenceIds().WhereNot(e => e == "persistenceInit");
        }

        private void Setup(string persistenceId, int n)
        {
            var @ref = Sys.ActorOf(Query.TestActor.Props(persistenceId));
            for (var i = 1; i <= n; i++)
            {
                @ref.Tell($"{persistenceId}-{i}");
                ExpectMsg($"{persistenceId}-{i}-done");
            }
        }

        [Fact]
        public void Cassandra_query_CurrentPersistenceIds_must_find_existing_events()
        {
            Setup("a", 1);
            Setup("b", 1);
            Setup("c", 1);

            var src = Current();
            src.RunWith(this.SinkProbe<string>(), _materializer)
                .Request(4)
                .ExpectNextUnordered("a", "b", "c")
                .ExpectComplete();
        }

        [Fact]
        public void
            Cassandra_query_CurrentPersistenceIds_must_deliver_persistenceId_only_once_if_there_are_multiple_events_spanning_partitions
            ()
        {
            Setup("d", 100);

            var src = Current();
            src.RunWith(this.SinkProbe<string>(), _materializer)
                .Request(10)
                .ExpectNext("d")
                .ExpectComplete();
        }


        [Fact]
        public void
            Cassandra_query_CurrentPersistenceIds_must_find_existing_persistence_ids_in_batches_if_there_is_more_of_them_than_max_result_size_query
            ()
        {
            for (var i = 1; i <= 1000; i++)
            {
                Setup(Guid.NewGuid().ToString(), 1);
            }

            var src = Current();
            var probe = src.RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(1000);

            for (var i = 1; i <= 1000; i++)
            {
                probe.ExpectNext();
            }

            probe.ExpectComplete();
        }

        [Fact]
        public void Cassandra_query_AllPersistenceIds_must_find_new_events()
        {
            Setup("e", 1);
            Setup("f", 1);

            var src = All();
            var probe = src.RunWith(this.SinkProbe<string>(), _materializer)
                .Request(5)
                .ExpectNextUnordered("e", "f");

            Setup("g", 1);

            probe.ExpectNext("g");
        }

        [Fact]
        public void Cassandra_query_AllPersistenceIds_must_find_new_events_after_demand_request()
        {
            Setup("h", 1);
            Setup("i", 1);

            var src = All();
            var probe = src.RunWith(this.SinkProbe<string>(), _materializer);

            probe.Request(1);
            probe.ExpectNext();
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            Setup("j", 1);

            probe.Request(5);
            probe.ExpectNext();
            probe.ExpectNext();
        }

        [Fact]
        public void Cassandra_query_AllPersistenceIds_must_only_deliver_what_requested_if_there_is_more_in_the_buffer()
        {
            Setup("k", 1);
            Setup("l", 1);
            Setup("m", 1);
            Setup("n", 1);
            Setup("o", 1);

            var src = All();
            var probe = src.RunWith(this.SinkProbe<string>(), _materializer);
            probe.Request(2);
            probe.ExpectNext();
            probe.ExpectNext();
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            probe.Request(2);
            probe.ExpectNext();
            probe.ExpectNext();
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(1000));
        }

        [Fact]
        public void Cassandra_query_AllPersistenceIds_must_deliver_persistenceId_only_once_if_there_are_multiple_events_spanning_partitions()
        {
            Setup("p", 1000);

            var src = All();
            var probe = src.RunWith(this.SinkProbe<string>(), _materializer);

            probe
                .Request(10)
                .ExpectNext("p")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            Setup("q", 1000);

            probe
                .Request(10)
                .ExpectNext("q")
                .ExpectNoMsg(TimeSpan.FromMilliseconds(1000));
        }
    }
}
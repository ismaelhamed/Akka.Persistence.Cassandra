//-----------------------------------------------------------------------
// <copyright file="AllPersistenceIdsPublisher.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Util;
using Cassandra;
using Finished = Akka.Persistence.Cassandra.Query.QueryActorPublisher.Finished;
using IAction = Akka.Persistence.Cassandra.Query.QueryActorPublisher.IAction;

namespace Akka.Persistence.Cassandra.Query
{
    internal class AllPersistenceIdsPublisher : QueryActorPublisher<string, AllPersistenceIdsState>
    {
        public static Props Props(TimeSpan? refreshInterval, AllPersistenceIdsSession session,
            CassandraReadJournalConfig config)
        {
            return Actor.Props.Create(() => new AllPersistenceIdsPublisher(refreshInterval, session, config));
        }


        public AllPersistenceIdsPublisher(TimeSpan? refreshInterval, AllPersistenceIdsSession session,
            CassandraReadJournalConfig config) : base(refreshInterval, config)
        {
            Session = session;
        }

        public AllPersistenceIdsSession Session { get; }

        protected override Task<AllPersistenceIdsState> InitialState()
        {
            return Task.FromResult(new AllPersistenceIdsState(ImmutableHashSet<string>.Empty));
        }

        protected override Task<IAction> InitialQuery(AllPersistenceIdsState initialState)
        {
            return Query();
        }

        protected override Task<IAction> RequestNext(AllPersistenceIdsState state, RowSet resultSet)
        {
            return Query();
        }

        protected override Task<IAction> RequestNextFinished(AllPersistenceIdsState state, RowSet resultSet)
        {
            return RequestNext(state, resultSet);
        }

        protected override Tuple<Option<string>, AllPersistenceIdsState> UpdateState(AllPersistenceIdsState state,
            Row row)
        {
            var @event = row.GetValue<string>("persistence_id");

            if (state.KnownPersistenceIds.Contains(@event))
            {
                return Tuple.Create(Option<string>.None, state);
            }
            return Tuple.Create(new Option<string>(@event),
                new AllPersistenceIdsState(state.KnownPersistenceIds.Add(@event)));
        }

        protected override bool CompletionCondition(AllPersistenceIdsState state)
        {
            return false;
        }

        private async Task<IAction> Query()
        {
            var boundStatement = Session.SelectDistinctPersistenceIds.Bind();
            boundStatement.SetPageSize(Config.FetchSize);

            var resultSet = await Session.Session.ExecuteAsync(boundStatement);
            return new Finished(resultSet);
        }
    }

    internal sealed class AllPersistenceIdsSession : INoSerializationVerificationNeeded
    {
        public AllPersistenceIdsSession(PreparedStatement selectDistinctPersistenceIds, ISession session)
        {
            SelectDistinctPersistenceIds = selectDistinctPersistenceIds;
            Session = session;
        }

        public PreparedStatement SelectDistinctPersistenceIds { get; }
        public ISession Session { get; }
    }

    internal sealed class AllPersistenceIdsState
    {
        public AllPersistenceIdsState(IImmutableSet<string> knownPersistenceIds)
        {
            KnownPersistenceIds = knownPersistenceIds;
        }

        public IImmutableSet<string> KnownPersistenceIds { get; }
    }
}
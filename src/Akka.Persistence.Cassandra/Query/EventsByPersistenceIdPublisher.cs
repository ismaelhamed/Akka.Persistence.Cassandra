//-----------------------------------------------------------------------
// <copyright file="EventsByPersistenceIdPublisher.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Cassandra.Journal;
using Cassandra;
using Finished = Akka.Persistence.Cassandra.Query.QueryActorPublisher.Finished;
using IAction = Akka.Persistence.Cassandra.Query.QueryActorPublisher.IAction;
using NewResultSet = Akka.Persistence.Cassandra.Query.QueryActorPublisher.NewResultSet;

namespace Akka.Persistence.Cassandra.Query
{
    internal class EventsByPersistenceIdPublisher : QueryActorPublisher<Persistent, EventsByPersistenceIdState>
    {
        public static Props Props(string persistenceId, long fromSequenceNr, long toSequenceNr, long max, int pageSize,
            TimeSpan? refreshInterval, EventsByPersistenceIdSession session, CassandraReadJournalConfig config)
        {
            return
                Actor.Props.Create(
                    () =>
                        new EventsByPersistenceIdPublisher(persistenceId, fromSequenceNr, toSequenceNr, max, pageSize,
                            refreshInterval, session, config));
        }

        private readonly Akka.Serialization.Serialization _serialization;

        public EventsByPersistenceIdPublisher(string persistenceId, long fromSequenceNr, long toSequenceNr, long max,
            int fetchSize, TimeSpan? refreshInterval, EventsByPersistenceIdSession session,
            CassandraReadJournalConfig config) : base(refreshInterval, config)
        {
            PersistenceId = persistenceId;
            FromSequenceNr = fromSequenceNr;
            ToSequenceNr = toSequenceNr;
            Max = max;
            FetchSize = fetchSize;
            Session = session;

            _serialization = Context.System.Serialization;
        }

        public string PersistenceId { get; }
        public long FromSequenceNr { get; }
        public long ToSequenceNr { get; }
        public long Max { get; }
        public int FetchSize { get; }
        public EventsByPersistenceIdSession Session { get; }

        protected override async Task<EventsByPersistenceIdState> InitialState()
        {

            var highestDeletedSequenceNr = await HighestDeletedSequenceNumber(PersistenceId);
            var initialFromSequenceNr = Math.Max(highestDeletedSequenceNr + 1, FromSequenceNr);
            var currentPartitionNumber = PartitionNr(initialFromSequenceNr, Config.TargetPartitionSize) + 1;

            return new EventsByPersistenceIdState(initialFromSequenceNr, 0, currentPartitionNumber);
        }

        protected override Task<IAction> InitialQuery(EventsByPersistenceIdState initialState)
        {
            return Query(new EventsByPersistenceIdState(initialState.Progress, initialState.Count,
                initialState.PartitionNr - 1));
        }

        protected override async Task<IAction> RequestNext(EventsByPersistenceIdState state, RowSet resultSet)
        {
            var inUse = await InUse(PersistenceId, state.PartitionNr);
            return inUse ? await Query(state) : new Finished(resultSet);
        }

        protected override async Task<IAction> RequestNextFinished(EventsByPersistenceIdState state, RowSet resultSet)
        {
            var action = await Query(new EventsByPersistenceIdState(state.Progress, state.Count, state.PartitionNr - 1));
            var newResultSet = ((NewResultSet) action).ResultSet;
            return newResultSet.IsExhausted() ? await RequestNext(state, resultSet) : action;
        }

        protected override Tuple<Persistent, EventsByPersistenceIdState> UpdateState(
            EventsByPersistenceIdState state, Row row)
        {
            var @event = ExtractEvent(row);
            var partitionNr = row.GetValue<long>("partition_nr") + 1;
            return Tuple.Create(@event,
                new EventsByPersistenceIdState(@event.SequenceNr + 1, state.Count + 1, partitionNr));
        }

        protected override bool CompletionCondition(EventsByPersistenceIdState state)
        {
            return state.Progress > ToSequenceNr || state.Count >= Max;
        }

        private Persistent ExtractEvent(Row row)
        {
            var bytes = row.GetValue<byte[]>("message");
            if (bytes != null)
            {
                // for backwards compatibility
                return PersistentFromBytes(_serialization, bytes);
            }

            return new Persistent(
                CassandraJournal.DeserializeEvent(_serialization, row),
                row.GetValue<long>("sequence_nr"),
                row.GetValue<string>("persistence_id"),
                row.GetValue<string>("event_manifest"),
                false,
                null,
                row.GetValue<string>("writer_uuid")
                );
        }

        private static Persistent PersistentFromBytes(Akka.Serialization.Serialization serialization, byte[] bytes)
        {
            return
                (Persistent) serialization.FindSerializerFor(typeof(Persistent)).FromBinary(bytes, typeof(Persistent));
        }

        private async Task<bool> InUse(string persistenceId, long currentPartitionNr)
        {
            var resultSet = await Session.SelectInUse(persistenceId, currentPartitionNr);
            return !resultSet.IsExhausted() && resultSet.First().GetValue<bool>("used");
        }

        private async Task<long> HighestDeletedSequenceNumber(string partitionKey)
        {
            var resultSet = await Session.SelectDeletedTo(partitionKey);
            return resultSet.FirstOrDefault()?.GetValue<long>("deleted_to") ?? 0;
        }

        private static long PartitionNr(long sequenceNr, int targetPartitionSize) =>
            (sequenceNr - 1L)/targetPartitionSize;

        private async Task<IAction> Query(EventsByPersistenceIdState state)
        {
            var resultSet =
                await
                    Session.SelectEventsByPersistenceId(PersistenceId, state.PartitionNr, state.Progress, ToSequenceNr,
                        FetchSize);
            return new NewResultSet(resultSet);
        }
    }

    internal class EventsByPersistenceIdSession
    {
        public PreparedStatement SelectEventsByPersistenceIdQuery { get; }
        public PreparedStatement SelectInUseQuery { get; }
        public PreparedStatement SelectDeletedToQuery { get; }
        public ISession Session { get; }
        public ConsistencyLevel? CustomConsistencyLevel { get; }

        public EventsByPersistenceIdSession(PreparedStatement selectEventsByPersistenceIdQuery,
            PreparedStatement selectInUseQuery, PreparedStatement selectDeletedToQuery, ISession session,
            ConsistencyLevel? customConsistencyLevel)
        {
            SelectEventsByPersistenceIdQuery = selectEventsByPersistenceIdQuery;
            SelectInUseQuery = selectInUseQuery;
            SelectDeletedToQuery = selectDeletedToQuery;
            Session = session;
            CustomConsistencyLevel = customConsistencyLevel;
        }

        public Task<RowSet> SelectEventsByPersistenceId(string persistenceId, long partitionNr, long progress,
            long toSequenceNr, int fetchSize)
        {
            var boundStatement = SelectEventsByPersistenceIdQuery.Bind(persistenceId, partitionNr, progress,
                toSequenceNr);
            boundStatement.SetPageSize(fetchSize);
            return ExecuteStatement(boundStatement);
        }

        public Task<RowSet> SelectInUse(string persistenceId, long currentPartitionNr)
            => ExecuteStatement(SelectInUseQuery.Bind(persistenceId, currentPartitionNr));

        public Task<RowSet> SelectDeletedTo(string partitionKey)
            => ExecuteStatement(SelectDeletedToQuery.Bind(partitionKey));

        private Task<RowSet> ExecuteStatement(IStatement statement)
        {
            return Session.ExecuteAsync(WithCustomConsistencyLevel(statement));
        }

        private IStatement WithCustomConsistencyLevel(IStatement statement)
        {
            return CustomConsistencyLevel.HasValue
                ? statement.SetConsistencyLevel(CustomConsistencyLevel.Value)
                : statement;
        }
    }

    internal class EventsByPersistenceIdState
    {
        public long Progress { get; }
        public long Count { get; }
        public long PartitionNr { get; }

        public EventsByPersistenceIdState(long progress, long count, long partitionNr)
        {
            Progress = progress;
            Count = count;
            PartitionNr = partitionNr;
        }
    }
}
//-----------------------------------------------------------------------
// <copyright file="CassandraReadJournal.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Cassandra.Journal;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Util;
using Cassandra;

namespace Akka.Persistence.Cassandra.Query
{
    /// <summary>
    /// <para>
    /// <see cref="IReadJournal"/> implementation for Cassandra.
    /// </para>
    /// <para>
    /// Is is retrieved with
    /// <code>CassandraReadJournal queries = PersistenceQuery.Get(system).GetReadJournalFor&lt;CassandraReadJournal&gt;(CassandraReadJournal.Identifier);</code>
    /// </para>
    /// <para>
    /// Configuration settings can be defined in the configuration section with the
    /// absolute path corresponding to the identifier, which is &quot;cassandra-query-journal&quot;
    /// for the default <see cref="Identifier"/>. See 'reference.conf'.
    /// </para>
    /// </summary>
    public class CassandraReadJournal : IAllPersistenceIdsQuery, ICurrentPersistenceIdsQuery,
        IEventsByPersistenceIdQuery, ICurrentEventsByPersistenceIdQuery, IEventsByTagQuery, ICurrentEventsByTagQuery
    {
        /// <summary>
        /// <para>
        /// The default identifier for <see cref="CassandraReadJournal"/> to be used with
        /// <see cref="PersistenceQuery.ReadJournalFor{TJournal}"/>.
        /// </para>
        /// <para>
        /// The value is &quot;cassandra-query-journal&quot; and corresponds
        /// to the absolute path to the read journal configuration entry.
        /// </para>
        /// </summary>
        public const string Identifier = "cassandra-query-journal";

        private sealed class CombinedEventsByPersistenceIdStatements
        {
            public CombinedEventsByPersistenceIdStatements(PreparedStatement preparedSelectEventsByPersistenceId,
                PreparedStatement preparedSelectInUse, PreparedStatement preparedSelectDeletedTo)
            {
                PreparedSelectEventsByPersistenceId = preparedSelectEventsByPersistenceId;
                PreparedSelectInUse = preparedSelectInUse;
                PreparedSelectDeletedTo = preparedSelectDeletedTo;
            }

            public PreparedStatement PreparedSelectEventsByPersistenceId { get; }
            public PreparedStatement PreparedSelectInUse { get; }
            public PreparedStatement PreparedSelectDeletedTo { get; }
        }

        private readonly ILoggingAdapter _log;
        private readonly CassandraJournalConfig _writePluginConfig;
        private readonly CassandraReadJournalConfig _queryPluginConfig;
        private readonly EventAdapters _eventAdapters;
        private readonly CassandraStatements _writeStatements;
        private readonly CassandraReadStatements _queryStatements;
        private readonly CassandraSession _session;
        private readonly Lazy<List<Task<PreparedStatement>>> _preparedSelectEventsByTag;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectEventsByPersistenceId;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectInUse;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectDeletedTo;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectDistinctPersistenceIds;
        private readonly Lazy<Task<CombinedEventsByPersistenceIdStatements>> _combinedEventsByPersistentIdStatements;

        public CassandraReadJournal(ExtendedActorSystem system, Config config)
        {
            _log = Logging.GetLogger(system, GetType());
            var writePluginId = config.GetString("write-plugin");
            _writePluginConfig = new CassandraJournalConfig(system, system.Settings.Config.GetConfig(writePluginId));
            _queryPluginConfig = new CassandraReadJournalConfig(config, _writePluginConfig);
            _eventAdapters = system.PersistenceExtension().AdaptersFor(writePluginId);
            _writeStatements = new CassandraStatements(_writePluginConfig);
            _queryStatements = new CassandraReadStatements(_queryPluginConfig);
            _session = new CassandraSession(system, _writePluginConfig, _log, null,
                session =>
                    _writeStatements.ExecuteCreateKeyspaceAndTables(session, _writePluginConfig,
                        _writePluginConfig.MaxTagId));
            _preparedSelectEventsByTag = new Lazy<List<Task<PreparedStatement>>>(() =>
            {
                return Enumerable.Range(1, _writePluginConfig.MaxTagId)
                    .Select(tagId => Prepare(_queryStatements.SelectEventsByTag(tagId)))
                    .ToList();
            });
            _preparedSelectEventsByPersistenceId = PrepareLazy(_writeStatements.SelectMessages);
            _preparedSelectInUse = PrepareLazy(_writeStatements.SelectInUse);
            _preparedSelectDeletedTo = PrepareLazy(_writeStatements.SelectDeletedTo);
            _preparedSelectDistinctPersistenceIds = PrepareLazy(_queryStatements.SelectDistinctPersistenceIds);
            _combinedEventsByPersistentIdStatements = new Lazy<Task<CombinedEventsByPersistenceIdStatements>>(async () =>
            {
                var preparedStatements =
                    await
                        Task.WhenAll(_preparedSelectEventsByPersistenceId.Value, _preparedSelectInUse.Value,
                            _preparedSelectDeletedTo.Value);
                return new CombinedEventsByPersistenceIdStatements(preparedStatements[0], preparedStatements[1],
                    preparedStatements[2]);
            });

            system.RegisterOnTermination(() => _session.Close());

            // FIXME perhaps we can do something smarter, such as caching the highest offset retrieved
            // from queries
            FirstOffset = _queryPluginConfig.FirstTimeBucket.StartTicks.StartOf();
        }

        /// <summary>
        /// Use this as the UUID offset in 'EventsByTag' queries when you want all
        /// events from the beginning of time.
        /// </summary>
        public Guid FirstOffset { get; }

        /// <summary>
        /// Create a time based Guid that can be used as offset in 'EventsByTag'
        /// queries.
        /// </summary>
        /// <param name="ticks">Value as returned by <see cref="DateTime.Ticks"/></param>
        public Guid OffsetGuid(long ticks)
        {
            return ticks == 0L ? FirstOffset : (Guid)ticks.StartOf();
        }


        /// <summary>
        /// <para>
        /// <see cref="AllPersistenceIds"/> is used to retrieve a stream of 'persistenceId's.
        /// </para>
        /// <para>
        /// The stream emits 'persistenceId' strings.
        /// </para>
        /// <para>
        /// The stream guarantees that a 'persistenceId' is only emitted once and there are no duplicates.
        /// Order is not defined. Multiple executions of the same stream (even bounded) may emit different
        /// sequence of 'persistenceId's.
        /// </para>
        /// <para>
        /// The stream is not completed when it reaches the end of the currently known 'persistenceId's,
        /// but it continues to push new 'persistenceId's when new events are persisted.
        /// Corresponding query that is completed when it reaches the end of the currently
        /// known 'persistenceId's is provided by 'currentPersistenceIds'.
        /// </para>
        /// <para>
        /// Note the query is inefficient, especially for large numbers of 'persistenceId's, because
        /// of limitation of current internal implementation providing no information supporting
        /// ordering/offset queries. The query uses Cassandra's 'select distinct' capabilities.
        /// More importantly the live query has to repeatedly execute the query each 'refresh-interval',
        /// because order is not defined and new 'persistenceId's may appear anywhere in the query results.
        /// </para>
        /// </summary>
        public Source<string, NotUsed> AllPersistenceIds() => PersistenceIds(_queryPluginConfig.RefreshInterval, "allPersistenceIds");


        /// <summary>
        /// Same type of query as <see cref="AllPersistenceIds"/> but the event stream
        /// is completed immediately when it reaches the end of the "result set". Events that are
        /// stored after the query is completed are not included in the event stream.
        /// </summary>
        public Source<string, NotUsed> CurrentPersistenceIds() => PersistenceIds(null, "currentPersistenceIds");

        /// <summary>
        /// <para>
        /// <see cref="EventsByPersistenceId"/> is used to retrieve a stream of events for a particular persistenceId.
        /// </para>
        /// <para>
        /// In addition to the 'offset' the 'EventEnvelope' also provides 'persistenceId' and 'sequenceNr'
        /// for each event. The 'sequenceNr' is the sequence number for the persistent actor with the
        /// 'persistenceId' that persisted the event. The 'persistenceId' + 'sequenceNr' is an unique
        /// identifier for the event.
        /// </para>
        /// <para>
        /// 'sequenceNr' and 'offset' are always the same for an event and they define ordering for events
        /// emitted by this query. Causality is guaranteed ('sequenceNr's of events for a particular
        /// 'persistenceId' are always ordered in a sequence monotonically increasing by one). Multiple
        /// executions of the same bounded stream are guaranteed to emit exactly the same stream of events.
        /// </para>
        /// <para>
        /// <paramref name="fromSequenceNr"/> and <paramref name="toSequenceNr"/> can be specified to limit the set of returned events.
        /// The <paramref name="fromSequenceNr"/> and <paramref name="toSequenceNr"/> are inclusive.
        /// </para>
        /// <para>
        /// Deleted events are also deleted from the event stream.
        /// </para>
        /// <para>
        /// The stream is not completed when it reaches the end of the currently stored events,
        /// but it continues to push new events when new events are persisted.
        /// Corresponding query that is completed when it reaches the end of the currently
        /// stored events is provided by 'currentEventsByPersistenceId'.
        /// </para>
        /// </summary>
        public Source<EventEnvelope, NotUsed> EventsByPersistenceId(string persistenceId, long fromSequenceNr, long toSequenceNr)
        {
            return
                EventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, long.MaxValue,
                    _queryPluginConfig.FetchSize, _queryPluginConfig.RefreshInterval,
                    $"eventsByPersistenceId-{persistenceId}").SelectMany(r => ToEventEnvelopes(r, r.SequenceNr));
        }

        /// <summary>
        /// Same type of query as <see cref="EventsByPersistenceId"/> but the event stream
        /// is completed immediately when it reaches the end of the "result set". Events that
        /// are stored after the query is completed are not included in the event stream.
        /// </summary>
        public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(string persistenceId, long fromSequenceNr,
            long toSequenceNr)
        {
            return
                EventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, long.MaxValue,
                    _queryPluginConfig.FetchSize, null, $"currentEventsByPersistenceId-{persistenceId}")
                    .SelectMany(r => ToEventEnvelopes(r, r.SequenceNr));
        }

        /// <summary>
        /// <para>
        /// 'EventsByTag' is used for retrieving events that were marked with
        /// a given tag, e.g. all events of an Aggregate Root type.
        /// </para>
        /// <para>
        /// To tag events you create an <see cref="IEventAdapter"/> that wraps the events
        /// in a <see cref="Tagged"/> with the given 'tags'.
        /// The tags must be defined in the 'tags' section of the 'cassandra-journal' configuration.
        /// Max 3 tags per event is supported.
        /// </para>
        /// <para>
        /// You can retrieve a subset of all events by specifying 'offset', or use '0L' to retrieve all
        /// events with a given tag. The 'offset' corresponds to a timesamp of the events. Note that the
        /// corresponding offset of each event is provided in the <see cref="EventEnvelope"/>,
        /// which makes it possible to resume the stream at a later point from a given offset.
        /// The 'offset' is inclusive, i.e. the events with the exact same timestamp will be included
        /// in the returned stream.
        /// </para>
        /// <para>
        /// There is a variant of this query with a time based Guid as offset. That query is better
        /// for cases where you want to resume the stream from an exact point without receiving
        /// duplicate events for the same timestamp.
        /// </para>
        /// <para>
        /// In addition to the 'offset' the <see cref="EventEnvelope"/> also provides 'persistenceId' and 'sequenceNr'
        /// for each event. The 'sequenceNr' is the sequence number for the persistent actor with the
        /// 'persistenceId' that persisted the event. The 'persistenceId' + 'sequenceNr' is an unique
        /// identifier for the event.
        /// </para>
        /// <para>
        /// The returned event stream is ordered by the offset (timestamp), which corresponds
        /// to the same order as the write journal stored the events, with inaccuracy due to clock skew
        /// between different nodes. The same stream elements (in same order) are returned for multiple
        /// executions of the query on a best effort basis. The query is using a Cassandra Materialized
        /// View for the query and that is eventually consistent, so different queries may see different
        /// events for the latest events, but eventually the result will be ordered by timestamp
        /// (Cassandra timeuuid column). To compensate for the the eventual consistency the query is
        /// delayed to not read the latest events, see 'cassandra-query-journal.eventual-consistency-delay'
        /// in reference.conf. However, this is only best effort and in case of network partitions
        /// or other things that may delay the updates of the Materialized View the events may be
        /// delivered in different order (not strictly by their timestamp).
        /// </para>
        /// <para>
        /// If you use the same tag for all events for a 'persistenceId' it is possible to get
        /// a more strict delivery order than otherwise. This can be useful when all events of
        /// a PersistentActor class (all events of all instances of that PersistentActor class)
        /// are tagged with the same tag. Then the events for each 'persistenceId' can be delivered
        /// strictly by sequence number. If a sequence number is missing the query is delayed up
        /// to the configured 'delayed-event-timeout' and if the expected event is still not
        /// found the stream is completed with failure. This means that there must not be any
        /// holes in the sequence numbers for a given tag, i.e. all events must be tagged
        /// with the same tag. Set 'delayed-event-timeout' to for example 30s to enable this
        /// feature. It is disabled by default.
        /// </para>
        /// <para>
        /// Deleted events are also deleted from the tagged event stream.
        /// </para>
        /// <para>
        /// The stream is not completed when it reaches the end of the currently stored events,
        /// but it continues to push new events when new events are persisted.
        /// Corresponding query that is completed when it reaches the end of the currently
        /// stored events is provided by 'CurrentEventsByTag'.
        /// </para>
        /// <para>
        /// The stream is completed with failure if there is a failure in executing the query in the
        /// backend journal.
        /// </para>
        /// </summary>
        public Source<EventEnvelope, NotUsed> EventsByTag(string tag, long offset)
        {
            return EventsByTag(tag, OffsetGuid(offset))
                .Select(
                    envelope =>
                        new EventEnvelope(((TimeUuid) envelope.Offset).GetDate().Ticks, envelope.PersistenceId,
                            envelope.SequenceNr, envelope.Event));
        }

        /// <summary>
        /// <para>
        /// Same type of query as <see cref="EventsByTag(string,long)"/>, but this one has
        /// a time based Guid <paramref name="offset"/>. This query is better for cases where you want to resume
        /// the stream from an exact point without receiving duplicate events for the same timestamp.
        /// </para>
        /// <para>
        /// Use <see cref="FirstOffset"/> when you want all events from the beginning of time.
        /// </para>
        /// <para>
        /// The <paramref name="offset"/> is exclusive, i.e. the event with the exact same Guid will not be included
        /// in the returned stream.
        /// </para>
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        public Source<GuidEventEnvelope, NotUsed> EventsByTag(string tag, Guid offset)
        {
            try
            {
                if (_writePluginConfig.EnableEventsByTagQuery)
                {
                    if (string.IsNullOrEmpty(tag))
                        throw new ArgumentException("tag must not be null or empty", nameof(tag));

                    return CreateSource(SelectStatement(tag), (s, ps) =>
                    {
                        return (Source<GuidEventEnvelope, NotUsed>)Source.ActorPublisher<GuidPersistent>(EventsByTagPublisher.Props(tag, offset,
                            Option<TimeUuid>.None, _queryPluginConfig, s, ps))
                            .SelectMany(r => ToGuidEventEnvelopes(r.Persistent, r.Offset))
                            .MapMaterializedValue(_ => NotUsed.Instance)
                            .Named("eventsByTag-" + HttpUtility.UrlEncode(tag, Encoding.UTF8))
                            .WithAttributes(ActorAttributes.CreateDispatcher(_queryPluginConfig.PluginDispatcher));
                    });
                }
                return Source.Failed<GuidEventEnvelope>(new NotSupportedException("EventsByTag query is disabled"));
            }
            catch (Exception e)
            {
                // e.g. from CassandraSession, or SelectStatement
                if (_log.IsDebugEnabled)
                    _log.Debug($"Could not run EventsByTag [{tag}] query, due to {e.Message}");
                return Source.Failed<GuidEventEnvelope>(e);
            }
        }

        /// <summary>
        /// <para>
        /// Same type of query as <see cref="EventsByTag(string,long)"/> but the event stream
        /// is completed immediately when it reaches the end of the "result set". Events that are
        /// stored after the query is completed are not included in the event stream.
        /// </para>
        /// <para>
        /// The <paramref name="offset"/> is inclusive, i.e. the events with the exact same timestamp
        /// will be included in the returned stream.
        /// </para>
        /// </summary>
        public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, long offset)
        {
            
            return CurrentEventsByTag(tag, OffsetGuid(offset))
                .Select(
                    envelope =>
                        new EventEnvelope(((TimeUuid) envelope.Offset).GetDate().Ticks, envelope.PersistenceId,
                            envelope.SequenceNr, envelope.Event));
        }

        /// <summary>
        /// <para>
        /// Same type of query as the <see cref="CurrentEventsByTag(string,long)"/> query, but this one has
        /// a time based Guid <paramref name="offset"/>. This query is better for cases where you want to resume
        /// the stream from an exact point without receiving duplicate events for the same timestamp.
        /// </para>
        /// <para>
        /// Use <see cref="FirstOffset"/> when you want all events from the beginning of time.
        /// </para>
        /// <para>
        /// The <paramref name="offset"/> is exclusive, i.e. the event with the exact same Guid will not be included
        /// in the returned stream.
        /// </para>
        /// </summary>
        public Source<GuidEventEnvelope, NotUsed> CurrentEventsByTag(string tag, Guid offset)
        {
            try
            {
                if (_writePluginConfig.EnableEventsByTagQuery)
                {
                    if (string.IsNullOrEmpty(tag))
                        throw new ArgumentException("tag must not be null or empty", nameof(tag));

                    var toOffset = OffsetGuid(DateTime.UtcNow.Ticks);
                    return CreateSource(SelectStatement(tag), (s, ps) =>
                    {
                        return (Source<GuidEventEnvelope, NotUsed>)Source.ActorPublisher<GuidPersistent>(EventsByTagPublisher.Props(tag, offset,
                            new Option<TimeUuid>(toOffset), _queryPluginConfig, s, ps))
                            .SelectMany(r => ToGuidEventEnvelopes(r.Persistent, r.Offset))
                            .MapMaterializedValue(_ => NotUsed.Instance)
                            .Named("currentEventsByTag-" + HttpUtility.UrlEncode(tag, Encoding.UTF8))
                            .WithAttributes(ActorAttributes.CreateDispatcher(_queryPluginConfig.PluginDispatcher));
                    });
                }
                return Source.Failed<GuidEventEnvelope>(new NotSupportedException("EventsByTag query is disabled"));
            }
            catch (Exception e)
            {
                // e.g. from CassandraSession, or SelectStatement
                if (_log.IsDebugEnabled)
                    _log.Debug($"Could not run EventsByTag [{tag}] query, due to {e.Message}");
                return Source.Failed<GuidEventEnvelope>(e);
            }
        }

        /// <summary>
        /// <para>
        /// This is a low-level method that return journal events as they are persisted.
        /// </para>
        /// <para>
        /// The fromJournal adaptation happens at higher level:
        /// <list type="bullet">
        /// <item><description>In the AsyncWriteJournal for the PersistentActor and PersistentView recovery.</description></item>
        /// <item><description>In the public eventsByPersistenceId and currentEventsByPersistenceId queries.</description></item>
        /// </list>
        /// </para>
        /// </summary>
        internal Source<Persistent, NotUsed> EventsByPersistenceId(string persistenceId, long fromSequenceNr,
            long toSequenceNr, long max, int pageSize, TimeSpan? refreshInterval, string name,
            ConsistencyLevel? customConsistencyLevel = null)
        {
            return
                CreateSource(
                    _combinedEventsByPersistentIdStatements.Value,
                    (s, c) =>
                        (Source<Persistent, NotUsed>) Source.ActorPublisher<Persistent>(
                            EventsByPersistenceIdPublisher.Props(
                                persistenceId,
                                fromSequenceNr,
                                toSequenceNr,
                                max,
                                pageSize,
                                refreshInterval,
                                new EventsByPersistenceIdSession(
                                    c.PreparedSelectEventsByPersistenceId,
                                    c.PreparedSelectInUse,
                                    c.PreparedSelectDeletedTo,
                                    s,
                                    customConsistencyLevel
                                    ),
                                _queryPluginConfig
                                )
                            )
                            .WithAttributes(ActorAttributes.CreateDispatcher(_queryPluginConfig.PluginDispatcher))
                            .MapMaterializedValue(_ => NotUsed.Instance)
                            .Named(name));
        }

        private Source<string, NotUsed> PersistenceIds(TimeSpan? refreshInterval, string name)
        {
            return
                CreateSource(
                    _preparedSelectDistinctPersistenceIds.Value,
                    (s, ps) =>
                        (Source<string, NotUsed>) Source.ActorPublisher<string>(
                            AllPersistenceIdsPublisher.Props(
                                refreshInterval,
                                new AllPersistenceIdsSession(ps, s),
                                _queryPluginConfig
                                )
                            )
                            .WithAttributes(ActorAttributes.CreateDispatcher(_queryPluginConfig.PluginDispatcher))
                            .MapMaterializedValue(_ => NotUsed.Instance)
                            .Named(name));
        }

        private Task<PreparedStatement> SelectStatement(string tag)
        {
            int tagId;
            if (!_writePluginConfig.Tags.TryGetValue(tag, out tagId))
                tagId = 1;
            return _preparedSelectEventsByTag.Value[tagId - 1];
        }

        private Lazy<Task<PreparedStatement>> PrepareLazy(string statement)
        {
            return new Lazy<Task<PreparedStatement>>(() => Prepare(statement));
        }

        private async Task<PreparedStatement> Prepare(string statement)
        {
            var preparedStatement = await _session.Prepare(statement);
            return preparedStatement.SetConsistencyLevel(_queryPluginConfig.ReadConsistency);
        }

        private Source<T, NotUsed> CreateSource<T, TStatement>(Task<TStatement> preparedStatement,
            Func<ISession, TStatement, Source<T, NotUsed>> source)
        {
            if (preparedStatement.IsCompleted)
            {
                if (preparedStatement.IsFaulted || preparedStatement.IsCanceled)
                {
                    return Source.Failed<T>(preparedStatement.IsFaulted
                        ? preparedStatement.Exception.Unwrap()
                        : new OperationCanceledException("Query canceled, possibly due to timing out."));
                }
                return source(GetSession(), preparedStatement.Result);
            }
            else
            {
                // completed later
                return Source.Maybe<TStatement>()
                    .MapMaterializedValue(promise =>
                    {
                        preparedStatement
                            .ContinueWith(t =>
                            {
                                if (t.IsCanceled)
                                    promise.SetCanceled();
                                else if (t.IsFaulted)
                                    promise.SetException(t.Exception.Flatten().InnerExceptions);
                                else
                                    promise.SetResult(t.Result);
                            });
                        return NotUsed.Instance;
                    })
                    .ConcatMany(ps => source(GetSession(), ps));
            }
        }

        private ISession GetSession()
        {
            // when we get the PreparedStatement we know that the session is initialized,
            // i.e. the 'Result' is safe
            return _session.Underlying.Result;
        }

        private IImmutableList<GuidEventEnvelope> ToGuidEventEnvelopes(Persistent persistent, Guid offset)
        {
            return AdaptFromJournal(persistent)
                .Select(
                    payload => new GuidEventEnvelope(offset, persistent.PersistenceId, persistent.SequenceNr, payload))
                .ToImmutableList();
        }

        private IImmutableList<EventEnvelope> ToEventEnvelopes(Persistent persistent, long offset)
        {
            return AdaptFromJournal(persistent)
                .Select(
                    payload => new EventEnvelope(offset, persistent.PersistenceId, persistent.SequenceNr, payload))
                .ToImmutableList();
        }

        private IEnumerable<object> AdaptFromJournal(Persistent persistent)
        {
            var eventAdapter = _eventAdapters.Get(persistent.Payload.GetType());
            var eventSequence = eventAdapter.FromJournal(persistent.Payload, persistent.Manifest);
            return eventSequence.Events;
        }
    }
}
//-----------------------------------------------------------------------
// <copyright file="EventsByTagFetcher.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Cassandra.Journal;
using Akka.Streams.Util;
using Cassandra;

namespace Akka.Persistence.Cassandra.Query
{
    internal class EventsByTagFetcher : ActorBase
    {
        #region Internal classes

        private sealed class InitResultSet : INoSerializationVerificationNeeded
        {
            public RowSet ResultSet { get; }

            public InitResultSet(RowSet resultSet)
            {
                ResultSet = resultSet;
            }
        }

        private sealed class Fetched : INoSerializationVerificationNeeded
        {
            public static readonly Fetched Instance = new Fetched();

            private Fetched()
            {
            }
        }

        #endregion

        public static Props Props(string tag, TimeBucket timeBucket, TimeUuid fromOffset, TimeUuid toOffset, int limit,
            bool backtracking, IActorRef replyTo, ISession session, PreparedStatement preparedSelect,
            Option<SequenceNumbers> sequenceNumbers, CassandraReadJournalConfig settings)
        {
            return
                Actor.Props.Create(
                    () =>
                        new EventsByTagFetcher(tag, timeBucket, fromOffset, toOffset, limit, backtracking, replyTo,
                            session, preparedSelect, sequenceNumbers, settings))
                    .WithDispatcher(settings.PluginDispatcher);
        }

        private static readonly IComparer<Guid> GuidComparer = new GuidComparer();
        private readonly Akka.Serialization.Serialization _serialization;
        private Guid _highestOffset;
        private int _count;
        private Option<SequenceNumbers> _sequenceNumbers;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        public EventsByTagFetcher(string tag, TimeBucket timeBucket, TimeUuid fromOffset, TimeUuid toOffset, int limit,
            bool backtracking, IActorRef replyTo, ISession session, PreparedStatement preparedSelect,
            Option<SequenceNumbers> sequenceNumbers, CassandraReadJournalConfig settings)
        {
            Tag = tag;
            TimeBucket = timeBucket;
            FromOffset = fromOffset;
            ToOffset = toOffset;
            Limit = limit;
            Backtracking = backtracking;
            ReplyTo = replyTo;
            Session = session;
            PreparedSelect = preparedSelect;
            Numbers = sequenceNumbers;
            Settings = settings;

            _serialization = Context.System.Serialization;
            _highestOffset = fromOffset;
            _count = 0;
            _sequenceNumbers = sequenceNumbers;
        }

        public string Tag { get; }
        public TimeBucket TimeBucket { get; }
        public TimeUuid FromOffset { get; }
        public TimeUuid ToOffset { get; }
        public int Limit { get; }
        public bool Backtracking { get; }
        public IActorRef ReplyTo { get; }
        public ISession Session { get; }
        public PreparedStatement PreparedSelect { get; }
        public Option<SequenceNumbers> Numbers { get; }
        public CassandraReadJournalConfig Settings { get; }

        protected override void PreStart()
        {
            var boundStatement = PreparedSelect.Bind(Tag, TimeBucket.Key, FromOffset, ToOffset, Limit);
            boundStatement.SetPageSize(Settings.FetchSize);
            var init = Session.ExecuteAsync(boundStatement);
            init
                .OnRanToCompletion(rs => new InitResultSet(rs))
                .PipeTo(Self);
        }

        protected override bool Receive(object message)
        {
            if (message is InitResultSet)
            {
                var initResultSet = (InitResultSet) message;
                Context.Become(Active(initResultSet.ResultSet));
                Continue(initResultSet.ResultSet);
            }
            else if (message is Status.Failure)
            {
                // from PipeTo
                throw ((Status.Failure) message).Cause;
            }
            else return false;
            return true;
        }

        private Receive Active(RowSet resultSet)
        {
            return message =>
            {
                if (message is Fetched)
                {
                    Continue(resultSet);
                }
                else if (message is Status.Failure)
                {
                    // from PipeTo
                    throw ((Status.Failure) message).Cause;
                }
                else return false;
                return true;
            };
        }

        private void Continue(RowSet resultSet)
        {
            if (resultSet.IsExhausted())
            {
                ReplyTo.Tell(new EventsByTagPublisher.ReplayDone(_count, _sequenceNumbers, _highestOffset));
                Context.Stop(Self);
            }
            else
            {
                var n = resultSet.GetAvailableWithoutFetching();
                while (true)
                {
                    if (n == 0)
                    {
                        var more = resultSet.FetchMoreResultsAsync();
                        more
                            .OnRanToCompletion(() => Fetched.Instance)
                            .PipeTo(Self);
                        break;
                    }

                    _count += 1;
                    var row = resultSet.First();
                    var persistenceId = row.GetValue<string>("persistence_id");
                    var sequenceNr = row.GetValue<long>("sequence_nr");

                    var offset = row.GetValue<Guid>("timestamp");
                    if (GuidComparer.Compare(offset, _highestOffset) <= 0)
                    {
                        if (_log.IsDebugEnabled)
                            _log.Debug(
                                "Events were not ordered by timestamp. Consider increasing eventual-consistency-delay if the order is of importance.");
                    }
                    else
                        _highestOffset = offset;

                    if (!_sequenceNumbers.HasValue)
                    {
                        ReplyTo.Tell(new GuidPersistent(offset, ToPersistent(row, persistenceId, sequenceNr)));
                        n -= 1;
                    }
                    else
                    {
                        var s = _sequenceNumbers.Value;
                        var exitLoop = false;
                        switch (s.IsNext(persistenceId, sequenceNr))
                        {
                            case SequenceNumbers.Answer.Yes:
                            case SequenceNumbers.Answer.PossiblyFirst:
                                _sequenceNumbers = s.Updated(persistenceId, sequenceNr);
                                ReplyTo.Tell(new GuidPersistent(offset, ToPersistent(row, persistenceId, sequenceNr)));
                                n -= 1;
                                break;
                            case SequenceNumbers.Answer.After:
                                ReplyTo.Tell(new EventsByTagPublisher.ReplayAborted(_sequenceNumbers, persistenceId,
                                    s.Get(persistenceId) + 1, sequenceNr));
                                // end loop
                                exitLoop = true;
                                break;
                            case SequenceNumbers.Answer.Before:
                                // duplicate, discard
                                if (!Backtracking)
                                {
                                    if (_log.IsDebugEnabled)
                                        _log.Debug(
                                            $"Discarding duplicate. Got sequence number [{sequenceNr}] for [{persistenceId}], but current sequence number is [{s.Get(persistenceId)}]");
                                }
                                n -= 1;
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                        if (exitLoop)
                            break;
                    }
                }
            }
        }

        private Persistent PersistentFromBytes(byte[] bytes)
        {
            return (Persistent) _serialization.FindSerializerForType(typeof(Persistent)).FromBinary(bytes, typeof(Persistent));
        }

        private Persistent ToPersistent(Row row, string persistenceId, long sequenceNr)
        {
            var bytes = row.GetValue<byte[]>("message");
            if (bytes == null)
                return new Persistent(CassandraJournal.DeserializeEvent(_serialization, row),
                    sequenceNr,
                    persistenceId,
                    row.GetValue<string>("event_manifest"),
                    false,
                    null,
                    row.GetValue<string>("writer_uuid"));
            // for backwards compatibility
            return PersistentFromBytes(bytes);
        }
    }
}
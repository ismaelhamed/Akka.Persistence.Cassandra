//-----------------------------------------------------------------------
// <copyright file="EventsByTagPublisher.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Event;
using Akka.Pattern;
using Akka.Persistence.Cassandra.Journal;
using Akka.Streams.Actors;
using Akka.Util.Internal;
using Cassandra;

namespace Akka.Persistence.Cassandra.Query
{
    internal class EventsByTagPublisher : ActorPublisher<GuidPersistent>
    {
        #region Internal classes

        public sealed class Continue
        {
            public static readonly Continue Instance = new Continue();

            private Continue()
            {
            }
        }

        public sealed class ReplayDone
        {
            public int Count { get; }
            public SequenceNumbers SequenceNumbers { get; }
            public Guid Highest { get; }

            public ReplayDone(int count, SequenceNumbers sequenceNumbers, Guid highest)
            {
                Count = count;
                SequenceNumbers = sequenceNumbers;
                Highest = highest;
            }
        }

        public sealed class ReplayAborted
        {
            public SequenceNumbers SequenceNumbers { get; }
            public string PersistenceId { get; }
            public long ExpectedSequenceNr { get; }
            public long GotSequenceNr { get; }

            public ReplayAborted(SequenceNumbers sequenceNumbers, string persistenceId, long expectedSequenceNr,
                long gotSequenceNr)
            {
                SequenceNumbers = sequenceNumbers;
                PersistenceId = persistenceId;
                ExpectedSequenceNr = expectedSequenceNr;
                GotSequenceNr = gotSequenceNr;
            }
        }

        public sealed class ReplayFailed : INoSerializationVerificationNeeded
        {
            public Exception Cause { get; }

            public ReplayFailed(Exception cause)
            {
                Cause = cause;
            }
        }

        #endregion

        public static Props Props(string tag, TimeUuid fromOffset, TimeUuid? toOffset,
            CassandraReadJournalConfig settings, ISession session, PreparedStatement preparedSelect)
        {
            return
                Actor.Props.Create(
                    () => new EventsByTagPublisher(tag, fromOffset, toOffset, settings, session, preparedSelect));
        }

        private static readonly IComparer<Guid> GuidComparer = new GuidComparer();
        private readonly long _eventualConsistencyDelayTicks;
        private readonly long _toOffsetTicks;
        private TimeBucket _currentTimeBucket;
        private TimeUuid _currentOffset;
        private TimeUuid _highestOffset;
        private readonly bool _strictBySequenceNumber;
        private SequenceNumbers _sequenceNumbers;
        private long? _abortDeadline;
        private readonly ICancelable _tickTask;
        private long _lookForMissingDeadline;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        public EventsByTagPublisher(string tag, TimeUuid fromOffset, TimeUuid? toOffset,
            CassandraReadJournalConfig settings, ISession session, PreparedStatement preparedSelect)
        {
            Tag = tag;
            FromOffset = fromOffset;
            ToOffset = toOffset;
            Settings = settings;
            Session = session;
            PreparedSelect = preparedSelect;

            Buffer = ImmutableArray<GuidPersistent>.Empty;

            _eventualConsistencyDelayTicks = settings.EventualConsistencyDelay.Ticks;
            _toOffsetTicks = toOffset?.GetDate().Ticks + _eventualConsistencyDelayTicks ?? long.MaxValue;

            // Subscribe to DistributedPubSub so we can receive immediate notifications when the journal has written something.
            if (settings.PubsubMinimumInterval.HasValue && settings.PubsubMinimumInterval != Timeout.InfiniteTimeSpan)
            {
                try
                {
                    DistributedPubSub.Get(Context.System)
                        .Mediator.Tell(new Subscribe("akka.persistence.cassandra.journal.tag", Self));
                }
                catch
                {
                    // Ignore pubsub when clustering unavailable
                }
            }

            _currentTimeBucket = new TimeBucket(FromOffset);
            _currentOffset = FromOffset;
            _highestOffset = FromOffset;
            _strictBySequenceNumber = settings.DelayedEventTimeout > TimeSpan.Zero;
            _sequenceNumbers = _strictBySequenceNumber
                ? SequenceNumbers.Empty
                : null;
            _abortDeadline = null;
            _lookForMissingDeadline = NextLookForMissingDeadline();
            _tickTask = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(settings.RefreshInterval,
                settings.RefreshInterval, Self, Continue.Instance, Self);
        }

        public string Tag { get; }
        public TimeUuid FromOffset { get; }
        public TimeUuid? ToOffset { get; }
        public CassandraReadJournalConfig Settings { get; }
        public ISession Session { get; }
        public PreparedStatement PreparedSelect { get; }

        protected ImmutableArray<GuidPersistent> Buffer { get; private set; }

        protected override void PreRestart(Exception reason, object message)
        {
            OnErrorThenStop(reason);
        }

        protected override void PostRestart(Exception reason)
        {
            throw new IllegalStateException($"{Self} must not be restarted");
        }

        protected override void PostStop()
        {
            _tickTask.Cancel();
        }

        protected override void Unhandled(object message)
        {
            if (message is string)
            {
                // These are published to the pubsub topic, and can be safely ignored if not specifically handled.
                return;
            }
            base.Unhandled(message);
        }

        public void DeliverBuffer()
        {
            if (Buffer.IsEmpty || TotalDemand <= 0) return;

            if (Buffer.Length == 1)
            {
                // optimize for this common case
                OnNext(Buffer[0]);
                Buffer = ImmutableArray<GuidPersistent>.Empty;
            }
            else if (TotalDemand <= int.MaxValue)
            {
                var totalDemand = (int) TotalDemand;
                Buffer.Take(totalDemand).ForEach(OnNext);
                Buffer = Buffer.Skip(totalDemand).ToImmutableArray();
            }
            else
            {
                Buffer.ForEach(OnNext);
                Buffer = ImmutableArray<GuidPersistent>.Empty;
            }
        }

        protected override bool Receive(object message)
        {
            return Init(message);
        }

        private bool Init(object message)
        {
            if (message is Request)
            {
                Replay();
            }
            else if (message is Continue)
            {
                // skip, wait for first Request
            }
            else if (message is Cancel)
            {
                Context.Stop(Self);
            }
            else return false;
            return true;
        }

        private Receive Idle()
        {
            return message =>
            {
                if (message is Continue)
                {
                    if (_strictBySequenceNumber && !IsBacktracking && _lookForMissingDeadline < DateTime.UtcNow.Ticks)
                    {
                        // look for delayed events
                        GoBack();
                        _lookForMissingDeadline = NextLookForMissingDeadline();
                    }
                    if (IsTimeForReplay)
                        Replay();
                }
                else if (message is Request)
                {
                    DeliverBuffer();
                    StopIfDone();
                }
                else if (message is Cancel)
                    Context.Stop(Self);
                else if (message is string && Tag.Equals((string) message, StringComparison.InvariantCulture))
                {
                    if (_eventualConsistencyDelayTicks == 0)
                        Self.Tell(Continue.Instance);
                    else
                        Context.System.Scheduler.ScheduleTellOnce(Settings.EventualConsistencyDelay, Self,
                            Continue.Instance, Self);
                }
                else return false;
                return true;
            };
        }

        private bool IsTimeForReplay => !IsToOffsetDone && (IsBacktracking || Buffer.Length == 0 || Buffer.Length <= Settings.MaxBufferSize / 2);

        private bool IsToOffsetDone => ToOffset.HasValue && GuidComparer.Compare(_currentOffset, ToOffset.Value) > 0;

        private bool IsCurrentTimeAfterToOffset => ToOffset.HasValue && DateTime.UtcNow.Ticks > _toOffsetTicks;

        private void StopIfDone()
        {
            if (Buffer.Length == 0 && (IsToOffsetDone|| IsCurrentTimeAfterToOffset))
            {
                OnCompleteThenStop();
            }
        }

        private void Replay()
        {
            var backtracking = IsBacktracking;
            var limit = backtracking ? Settings.MaxBufferSize : Settings.MaxBufferSize - Buffer.Length;
            var toOffset = backtracking && !_abortDeadline.HasValue
                ? _highestOffset
                : DateTimeOffset.UtcNow.AddTicks(-_eventualConsistencyDelayTicks).EndOf();
            if (_log.IsDebugEnabled)
                _log.Debug($"{(backtracking ? "backtracking " : "")}query for tag [{Tag}] from [{_currentTimeBucket}] [{_currentOffset}] limit [{limit}]");
            Context.ActorOf(EventsByTagFetcher.Props(Tag, _currentTimeBucket, _currentOffset, toOffset, limit,
                backtracking, Self, Session, PreparedSelect, _sequenceNumbers, Settings));
            Context.Become(Replaying());
        }

        private Receive Replaying()
        {
            return message =>
            {
                if (message is GuidPersistent)
                {
                    var envelope = (GuidPersistent) message;
                    _currentOffset = envelope.Offset;
                    if (GuidComparer.Compare(_currentOffset, _highestOffset) > 0)
                        _highestOffset = _currentOffset;
                    if (IsToOffsetDone)
                        StopIfDone();
                    else
                        Buffer = Buffer.Add(envelope);
                    DeliverBuffer();
                }
                else if (message is ReplayDone)
                {
                    var replayDone = (ReplayDone) message;
                    var count = replayDone.Count;
                    if (_log.IsDebugEnabled)
                        _log.Debug($"query chunk done for tag [{Tag}], timeBucket [{_currentTimeBucket}], count [{count}]");
                    _sequenceNumbers = replayDone.SequenceNumbers;
                    _currentOffset = replayDone.Highest;
                    if (_currentOffset == _highestOffset)
                        _abortDeadline = null; // back on track again

                    DeliverBuffer();

                    if (count == 0)
                    {
                        if (IsTimeBucketBeforeToday)
                        {
                            NextTimeBucket();
                            Self.Tell(Continue.Instance); // more to fetch
                        }
                        else
                        {
                            StopIfDone();
                        }
                    }
                    else
                    {
                        Self.Tell(Continue.Instance); // more to fetch
                    }

                    Context.Become(Idle());
                }
                else if (message is ReplayAborted)
                {
                    var replayAborted = (ReplayAborted) message;
                    // this wil only happen when DelayedEventTimeout is > 0s
                    _sequenceNumbers = replayAborted.SequenceNumbers;
                    var logMessage =
                        $"query chunk aborted for tag [{Tag}], timeBucket [{_currentTimeBucket}], expected sequence number [{replayAborted.ExpectedSequenceNr}] for [{replayAborted.PersistenceId}], but got [{replayAborted.GotSequenceNr}]";
                    if (_abortDeadline.HasValue && _abortDeadline.Value < DateTime.UtcNow.Ticks)
                    {
                        _log.Error(logMessage);
                        OnErrorThenStop(new IllegalStateException(logMessage));
                    }
                    else
                    {
                        if (_log.IsDebugEnabled)
                            _log.Debug(logMessage);
                        if (!_abortDeadline.HasValue)
                            _abortDeadline = DateTime.UtcNow.Ticks + Settings.DelayedEventTimeout.Ticks;
                        // go back in history and look for missing sequence numbers
                        GoBack();
                        Context.Become(Idle());
                    }
                }
                else if (message is ReplayFailed)
                {
                    var cause = ((ReplayFailed) message).Cause;
                    if (_log.IsDebugEnabled)
                        _log.Debug($"query failed for tag [{Tag}], due to [{cause.Message}]");
                    DeliverBuffer();
                    OnErrorThenStop(cause);
                }
                else if (message is Request)
                {
                    DeliverBuffer();
                }
                else if (message is Continue)
                {
                    // skip during replay
                }
                else if (message is Cancel)
                {
                    Context.Stop(Self);
                }
                else return false;
                return true;
            };
        }

        private void NextTimeBucket()
        {
            _currentTimeBucket = _currentTimeBucket.Next();
        }

        private LocalDate Today
        {
            get
            {
                var offset = DateTimeOffset.UtcNow.AddTicks(-_eventualConsistencyDelayTicks);
                return new LocalDate(offset.Year, offset.Month, offset.Day);
            }
        }

        private bool IsTimeBucketBeforeToday => _currentTimeBucket.IsBefore(Today);

        private void GoBack()
        {
            var timestamp = _currentOffset.GetDate().AddTicks(-Settings.DelayedEventTimeout.Ticks);
            var backFromOffset = timestamp.StartOf();
            _currentOffset = FromOffset.CompareTo(backFromOffset) >= 0 ? FromOffset : backFromOffset;
            _currentTimeBucket = new TimeBucket(_currentOffset);
        }

        private bool IsBacktracking => GuidComparer.Compare(_currentOffset, _highestOffset) < 0;

        private long NextLookForMissingDeadline()
        {
            return DateTime.UtcNow.Ticks + Settings.DelayedEventTimeout.Ticks/2;
        }

        // exceptions from Fetcher
        protected override SupervisorStrategy SupervisorStrategy()
        {
            return new OneForOneStrategy(-1, -1, e =>
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Query of EventsByTag [{Tag}] failed, due to: {e.Message}");
                Self.Tell(new ReplayFailed(e));
                return Directive.Stop;
            }, false);
        }
    }
}
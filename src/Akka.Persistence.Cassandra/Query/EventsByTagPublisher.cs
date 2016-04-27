using System;
using Akka.Streams.Util;

namespace Akka.Persistence.Cassandra.Query
{
    internal class EventsByTagPublisher
    {
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
            public Option<SequenceNumbers> SequenceNumbers { get; }
            public Guid Highest { get; }

            public ReplayDone(int count, Option<SequenceNumbers> sequenceNumbers, Guid highest)
            {
                Count = count;
                SequenceNumbers = sequenceNumbers;
                Highest = highest;
            }
        }

        public sealed class ReplayAborted
        {
            public Option<SequenceNumbers> SequenceNumbers { get; }
            public string PersistenceId { get; }
            public long ExpectedSequenceNr { get; }
            public long GotSequenceNr { get; }

            public ReplayAborted(Option<SequenceNumbers> sequenceNumbers, string persistenceId, long expectedSequenceNr, long gotSequenceNr)
            {
                SequenceNumbers = sequenceNumbers;
                PersistenceId = persistenceId;
                ExpectedSequenceNr = expectedSequenceNr;
                GotSequenceNr = gotSequenceNr;
            }
        }

        public sealed class ReplayFailed
        {
            public Exception Cause { get; }

            public ReplayFailed(Exception cause)
            {
                Cause = cause;
            }
        }
    }
}
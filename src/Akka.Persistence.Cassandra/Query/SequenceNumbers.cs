﻿//-----------------------------------------------------------------------
// <copyright file="SequenceNumbers.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Immutable;

namespace Akka.Persistence.Cassandra.Query
{
    internal class SequenceNumbers
    {
        public enum Answer
        {
            Yes,
            Before,
            After,
            PossiblyFirst
        }

        public static readonly SequenceNumbers Empty = new SequenceNumbers(ImmutableDictionary<string, int>.Empty,
            ImmutableDictionary<string, long>.Empty);

        public SequenceNumbers(IImmutableDictionary<string, int> intNumbers, IImmutableDictionary<string, long> longNumbers)
        {
            LongNumbers = longNumbers;
            IntNumbers = intNumbers;
        }

        public IImmutableDictionary<string, int> IntNumbers { get; }
        public IImmutableDictionary<string, long> LongNumbers { get; }

        public Answer IsNext(string persistenceId, long sequenceNr)
        {
            var n = Get(persistenceId);
            if (sequenceNr == n + 1) return Answer.Yes;
            if (n == 0) return Answer.PossiblyFirst;
            if (sequenceNr > n + 1) return Answer.After;
            return Answer.Before;
        }

        public long Get(string persistenceId)
        {
            int n;
            if (IntNumbers.TryGetValue(persistenceId, out n))
                return n;

            long n2;
            if (LongNumbers.TryGetValue(persistenceId, out n2))
                return n2;
            return 0L;
        }

        public SequenceNumbers Updated(string persistenceId, long sequenceNr)
        {
            if (sequenceNr <= int.MaxValue)
                return new SequenceNumbers(IntNumbers.SetItem(persistenceId, (int) sequenceNr), LongNumbers);
            if (sequenceNr == 1L + int.MaxValue)
                return new SequenceNumbers(IntNumbers.Remove(persistenceId), LongNumbers.SetItem(persistenceId, sequenceNr));
            return new SequenceNumbers(IntNumbers, LongNumbers.SetItem(persistenceId, sequenceNr));
        }
    }
}
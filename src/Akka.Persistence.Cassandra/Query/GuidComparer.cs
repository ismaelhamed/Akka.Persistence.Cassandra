//-----------------------------------------------------------------------
// <copyright file="GuidComparer.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Akka.Persistence.Cassandra.Query
{
    // C# implementation of UUIDComparator in
    // https://github.com/cowtowncoder/java-uuid-generator
    // Apache License 2.0.
    public class GuidComparer : IComparer<Guid>
    {
        private static readonly int[] ByteOrderMostSignicantBits = {3, 2, 1, 0, 5, 4, 7, 6};
        private static readonly int VersionByteIndex = ByteOrderMostSignicantBits[6];
        private static readonly IComparer<ulong> UlongComparer = Comparer<ulong>.Default;

        public int Compare(Guid x, Guid y)
        {
            var xBytes = x.ToByteArray();
            var yBytes = y.ToByteArray();

            // First: major sorting by types
            var version = Version(xBytes);
            var diff = version - Version(yBytes);
            if (diff != 0)
                return diff;

            //Second: for time-based variant, order by time stamp:
            var xMostSignificantBits = MostSignificantBits(xBytes);
            var yMostSignificantBits = MostSignificantBits(yBytes);
            if (version == 1)
            {
                var diff2 = UlongComparer.Compare(Timestamp(xMostSignificantBits), Timestamp(yMostSignificantBits));
                if (diff2 != 0)
                    return diff2;
                // or if that didn't work, by other bits lexically
                return UlongComparer.Compare(LeastSignificantBits(xBytes), LeastSignificantBits(yBytes));
            }
            else
            {
                // note: java.util.UUIDs compare with sign extension, IMO that's wrong, so:
                var diff2 = UlongComparer.Compare(xMostSignificantBits, yMostSignificantBits);
                if (diff2 != 0)
                    return diff2;
                return UlongComparer.Compare(LeastSignificantBits(xBytes), LeastSignificantBits(yBytes));
            }
        }

        private static ulong MostSignificantBits(byte[] bytes)
        {
            ulong msb = 0L;
            for (var i=0; i<8; i++)
                msb = (msb << 8) | bytes[ByteOrderMostSignicantBits[i]];
            return msb;
        }

        private static ulong LeastSignificantBits(byte[] bytes)
        {
            ulong lsb = 0L;
            for (var i = 8; i < 16; i++)
                lsb = (lsb << 8) | bytes[i];
            return lsb;
        }

        private static int Version(byte[] bytes)
        {
            // Version is bits masked by 0x000000000000F000 in most significant long
            return (bytes[VersionByteIndex] >> 4) & 0x0f;
        }

        // The 60 bit timestamp value is constructed from the time_low,
        // time_mid, and time_hi fields of this Guid. The resulting
        // timestamp is measured in 100-nanosecond units since midnight,
        // October 15, 1582 UTC.
        private static ulong Timestamp(ulong mostSignificantBits)
        {
            return (mostSignificantBits & 0x0FFFL) << 48
                 | ((mostSignificantBits >> 16) & 0x0FFFFL) << 32
                 | mostSignificantBits >> 32;
        }
    }
}
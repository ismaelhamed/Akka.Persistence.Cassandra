//-----------------------------------------------------------------------
// <copyright file="GuidPersistent.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Akka.Persistence.Cassandra.Query
{
    /// <summary>
    /// Wrap the <see cref="Persistent"/> to add the Guid for
    /// `EventsByTag` query, or similar queries.
    /// </summary>
    internal sealed class GuidPersistent
    {
        public Guid Offset { get; }
        public Persistent Persistent { get; }

        public GuidPersistent(Guid offset, Persistent persistent)
        {
            Offset = offset;
            Persistent = persistent;
        }
    }
}
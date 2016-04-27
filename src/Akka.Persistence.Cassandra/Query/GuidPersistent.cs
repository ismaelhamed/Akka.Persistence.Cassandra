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
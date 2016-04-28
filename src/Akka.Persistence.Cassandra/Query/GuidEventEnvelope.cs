using System;

namespace Akka.Persistence.Cassandra.Query
{
    /// <summary>
    /// Event wrapper adding metadata for the events in the result stream
    /// of the `EventsByTag` query, or similar queries.
    /// </summary>
    public sealed class GuidEventEnvelope
    {
        public Guid Offset { get; }
        public string PersistenceId { get; }
        public long SequenceNr { get; }
        public object Event { get; }

        public GuidEventEnvelope(Guid offset, string persistenceId, long sequenceNr, object @event)
        {
            Offset = offset;
            PersistenceId = persistenceId;
            SequenceNr = sequenceNr;
            Event = @event;
        }
    }
}
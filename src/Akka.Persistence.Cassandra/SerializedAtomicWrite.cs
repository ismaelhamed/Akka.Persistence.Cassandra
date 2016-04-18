using System.Collections.Generic;

namespace Akka.Persistence.Cassandra
{
    public class SerializedAtomicWrite
    {
        public string PersistenceId { get; set; }
        public IEnumerable<Serialized> Payload {get; set; }
    }

    public class Serialized
    {
        public string PersistenceId { get; set; }
        public long SequenceNr { get; set; }
        public byte[] SerializedData { get; set; }
        public string[] Tags { get; set; }
        public string EventManifest { get; set; }
        public string SerManifest { get; set; }
        public int SerId { get; set; }
        public string WriterUuid { get; set; }
    }
}
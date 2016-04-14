namespace Akka.Persistence.Cassandra.Journal
{
    /// <summary>
    /// CQL strings for use with the CassandraJournal.
    /// </summary>
    internal static class JournalStatements
    {
        public const string CreateKeyspace = @"
            CREATE KEYSPACE IF NOT EXISTS {0}
            WITH {1}";

        public const string CreateConfigTable = @"
          CREATE TABLE IF NOT EXISTS {0} (
            property text primary key, value text)";
     

        public const string CreateTable = @"
          CREATE TABLE IF NOT EXISTS ${tableName} (
            used boolean static,
            persistence_id text,
            partition_number bigint,
            sequence_number bigint,
            timestamp timeuuid,
            timebucket text,
            writer_uuid text,
            ser_id int,
            ser_manifest text,
            event_manifest text,
            event blob,
            tag1 text,
            tag2 text,
            tag3 text,
            message blob,
            PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp, timebucket))";

        // WITH gc_grace_seconds =${config.gc_grace_seconds}
        //  AND compaction = ${config.tableCompactionStrategy.asCQL

        public const string WriteMessage = @"
      INSERT INTO ${tableName} (persistence_id, partition_number, sequence_number, timestamp, timebucket, writer_uuid, ser_id, ser_manifest, event_manifest, event, tag1, tag2, tag3, message, used)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , true)";

        public const string WriteHeader = @"
            INSERT INTO {0} (persistence_id, partition_number, marker, sequence_number)
            VALUES (?, ?, 'H', ?)";

        public const string SelectMessages = @"
            SELECT message FROM {0} WHERE persistence_id = ? AND partition_number = ?
            AND marker = 'A' AND sequence_number >= ? AND sequence_number <= ?";

        public const string WriteDeleteMarker = @"
            INSERT INTO {0} (persistence_id, partition_number, marker, sequence_number)
            VALUES (?, ?, 'D', ?)";

        public const string DeleteMessagePermanent = @"
            DELETE FROM {0} WHERE persistence_id = ? AND partition_number = ? 
            AND marker = 'A' AND sequence_number = ?";

        public const string SelectDeletedToSequence = @"
            SELECT sequence_number FROM {0} WHERE persistence_id = ? AND partition_number = ? 
            AND marker = 'D' ORDER BY marker DESC, sequence_number DESC LIMIT 1";

        public const string SelectLastMessageSequence = @"
            SELECT sequence_number FROM {0} WHERE persistence_id = ? AND partition_number = ? 
            AND marker = 'A' AND sequence_number >= ? 
            ORDER BY marker DESC, sequence_number DESC LIMIT 1";

        public const string SelectHeaderSequence = @"
            SELECT sequence_number FROM {0} WHERE persistence_id = ? AND partition_number = ? 
            AND marker = 'H'";

        public const string SelectConfigurationValue = @"
            SELECT message FROM {0}
            WHERE persistence_id = 'akkanet-configuration-values' AND partition_number = 0 AND marker = ?";

        public const string WriteConfigurationValue = @"
            INSERT INTO {0} (persistence_id, partition_number, marker, sequence_number, message) 
            VALUES ('akkanet-configuration-values', 0, ?, 0, ?)";
    }
}

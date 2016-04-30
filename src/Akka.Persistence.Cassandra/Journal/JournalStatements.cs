namespace Akka.Persistence.Cassandra.Journal
{
    /// <summary>
    /// CQL strings for use with the CassandraJournal.
    /// </summary>
    internal class JournalStatements
    {
        JournalStatements(CassandraJournalConfig seetings)
        {
            //seetings.ta
        }

        public const string CreateKeyspace = @"
            CREATE KEYSPACE IF NOT EXISTS {0}
            WITH {1}";

        public const string CreateConfigTable = @"
          CREATE TABLE IF NOT EXISTS {0} (
            property text primary key, value text)";
     

        public const string CreateTable = @"
          CREATE TABLE IF NOT EXISTS {0} (
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
            PRIMARY KEY ((persistence_id, partition_number), sequence_number, timestamp, timebucket))
            {1}{2}";

        // WITH gc_grace_seconds =${config.gc_grace_seconds}
        //  AND compaction = ${config.tableCompactionStrategy.asCQL

        public const string CreateMetatdataTable = @"
          CREATE TABLE IF NOT EXISTS {0}(
            persistence_id text PRIMARY KEY,
            deleted_to bigint,
            properties map<text, text>)";

        public static string CreateEventsByTagMaterializedView(string viewName, int tagId)
        {
            return $@"CREATE MATERIALIZED VIEW IF NOT EXISTS {viewName}{tagId} AS
                     SELECT tag{tagId}, timebucket, timestamp, persistence_id, partition_number, sequence_number, writer_uuid, ser_id, ser_manifest, event_manifest, event, message
                     FROM $tableName
                     WHERE persistence_id IS NOT NULL AND partition_number IS NOT NULL AND sequence_number IS NOT NULL
                       AND tag$tagId IS NOT NULL AND timestamp IS NOT NULL AND timebucket IS NOT NULL
                     PRIMARY KEY ((tag$tagId, timebucket), timestamp, persistence_id, partition_number, sequence_number)
                     WITH CLUSTERING ORDER BY (timestamp ASC)";
        }

        public const string WriteMessage = @"
          INSERT INTO {0} (persistence_id, partition_number, sequence_number, timestamp, timebucket, writer_uuid, ser_id, ser_manifest, event_manifest, event, tag1, tag2, tag3, message, used)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , true)";

//        public const string WriteHeader = @"
//            INSERT INTO {0} (persistence_id, partition_number, marker, sequence_number)
//            VALUES (?, ?, 'H', ?)";


        public const string DeleteMessage = @"
          DELETE FROM ${tableName} WHERE
            persistence_id = ? AND
            partition_number = ? AND
            sequence_number <= ?";

        public const string DeleteMessages = @"
            DELETE FROM {0} WHERE
                persistence_id = ? AND
                partition_number = ? AND
                sequence_number <= ?";

        public const string SelectMessages = @"
            SELECT * FROM {0} WHERE persistence_id = ? AND partition_number = ?
            AND sequence_number >= ? AND sequence_number <= ?";


        public const string SelectInUse = @"
            SELECT used from {0}
                WHERE persistence_id = ? AND 
                    partition_number = ?";

        public const string SelectConfig = @"SELECT * FROM {0}";

        public const string WriteConfig = @"
            INSERT INTO {0} (property, value) VALUES(?, ?) IF NOT EXISTS";

        public const string SelectDeletedTo = @"
            SELECT deleted_to FROM {0}
                WHERE persistence_id = ?";

        public const string InsertDeletedTo = @"
            INSERT INTO {0}
                (persistence_id, deleted_to)
                      VALUES( ?, ? )
                    ";

        public const string WriteInUse = @"
            INSERT INTO {0} (persistence_id, partition_nr, used)
                   VALUES(?, ?, true)";

        public const string SelectHighestSequenceNumber = @"
             SELECT sequence_nr, used FROM {0} WHERE
               persistence_id = ? AND
               partition_nr = ?
               ORDER BY sequence_nr
               DESC LIMIT 1";

    }
}

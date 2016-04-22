using System.Threading.Tasks;
using Cassandra;

namespace Akka.Persistence.Cassandra.Snapshot
{
    public class CassandraStatements
    {
        private readonly string _selectSnapshotMetadata;

        public CassandraStatements(CassandraSnapshotStoreConfig config)
        {
            var config1 = config;
            string tableName = $"{config.Keyspace}.{config.Table}";

            CreateKeyspace =
                $@"
CREATE KEYSPACE IF NOT EXISTS {config1.Keyspace}
WITH REPLICATION = {{ 'class' : {config1
                    .ReplicationStrategy} }}
";

            CreateTable =
                $@"
CREATE TABLE IF NOT EXISTS {tableName} (
persistence_id text,
    sequence_nr bigint,
    timestamp bigint,
    ser_id int,
    ser_manifest text,
    snapshot_data blob,
    snapshot blob,
    PRIMARY KEY (persistence_id, sequence_nr))
    WITH CLUSTERING ORDER BY (sequence_nr DESC)
    AND compaction = {config1
                    .TableCompactionStrategy.AsCQL}
";

            WriteSnapshot =
                $@"
INSERT INTO {tableName} (persistence_id, sequence_nr, timestamp, ser_manifest, ser_id, snapshot_data, snapshot)
VALUES (?, ?, ?, ?, ?, ?, ?)
";

            DeleteSnapshot =
                $@"
DELETE FROM {tableName} WHERE
    persistence_id = ? AND
    sequence_nr = ?
";

            SelectSnapshot = 
                $@"
SELECT * FROM {tableName} WHERE
    persistence_id = ? AND
    sequence_nr = ?
";

            _selectSnapshotMetadata =
                $@"
SELECT persistence_id, sequence_nr, timestamp FROM {tableName} WHERE
    persistence_id = ? AND
    sequence_nr <= ?
    {{0}}
";
        }

        public string CreateKeyspace { get; }
        public string CreateTable { get; }
        public string WriteSnapshot { get; }
        public string DeleteSnapshot { get; }
        public string SelectSnapshot { get; }

        public string SelectSnapshotMetadata(int? limit = null)
            => string.Format(_selectSnapshotMetadata, limit.HasValue ? $"LIMIT {limit.Value}" : string.Empty);

        /// <summary>
        /// Execute creation of keyspace and tables is limited to one thread at a time to
        /// reduce the risk of (annoying) "Column family ID mismatch" exception
        /// when write and read-side plugins are started at the same time.
        /// Those statements are retried, because that could happen across different
        /// nodes also but serializing those statements gives a better "experience".
        /// </summary>
        public Task ExecuteCreateKeyspaceAndTables(ISession session, CassandraSnapshotStoreConfig config)
        {
            return CassandraSession.SerializedExecution(
                () => ExecuteCreateKeyspaceAndTables(session, config),
                () => CreateKeySpaceAndTables(session, config)
                );
        }

        private Task CreateKeySpaceAndTables(ISession session, CassandraSnapshotStoreConfig config)
        {
            var keyspaceTask = config.KeyspaceAutocreate
                ? (Task) session.ExecuteAsync(new SimpleStatement(CreateKeyspace))
                : Task.FromResult(new object());

            if (config.TablesAutocreate)
                return keyspaceTask.ContinueWith(_ => session.ExecuteAsync(new SimpleStatement(CreateTable)),
                    TaskContinuationOptions.OnlyOnRanToCompletion);
            return keyspaceTask;
        }
    }
}
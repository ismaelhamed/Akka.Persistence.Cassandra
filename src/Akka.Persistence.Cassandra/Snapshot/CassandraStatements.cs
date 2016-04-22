using System.Threading.Tasks;
using Cassandra;

namespace Akka.Persistence.Cassandra.Snapshot
{
    public class CassandraStatements
    {
        private readonly CassandraSnapshotStoreConfig _config;
        private readonly string _tableName;

        public CassandraStatements(CassandraSnapshotStoreConfig config)
        {
            _config = config;
            _tableName = $"{config.Keyspace}.{config.Table}";
        }

        public string CreateKeyspace
            =>
                $@"
            CREATE KEYSPACE IF NOT EXISTS {_config.Keyspace}
            WITH REPLICATION = {{ 'class' : {_config
                    .ReplicationStrategy} }}";

        public string CreateTable
            =>
                $@"
            CREATE TABLE IF NOT EXISTS {_tableName} (
                persistence_id text,
                sequence_nr bigint,
                timestamp bigint,
                ser_id int,
                ser_manifest text,
                snapshot_data blob,
                snapshot blob,
                PRIMARY KEY (persistence_id, sequence_nr))
                WITH CLUSTERING ORDER BY (sequence_nr DESC)
                AND compaction = {_config
                    .TableCompactionStrategy.AsCQL}";

        public string WriteSnapshot
            =>
                $@"
            INSERT INTO {_tableName} (persistence_id, sequence_nr, timestamp, ser_manifest, ser_id, snapshot_data, snapshot)
            VALUES (?, ?, ?, ?, ?, ?, ?)"
            ;

        public string DeleteSnapshot
            =>
                $@"
            DELETE FROM {_tableName} WHERE
                persistence_id = ? AND
                sequence_nr = ?"
            ;

        public string SelectSnapshot
            =>
                $@"
            SELECT * FROM {_tableName} WHERE
                persistence_id = ? AND
                sequence_nr = ?";

        public string SelectSnapshotMetadata(int? limit = null)
            =>
                $@"
      SELECT persistence_id, sequence_nr, timestamp FROM {_tableName} WHERE
        persistence_id = ? AND
        sequence_nr <= ?
        {(limit
                    .HasValue
                    ? $"LIMIT {limit.Value}"
                    : string.Empty)}";

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
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace Akka.Persistence.Cassandra.Journal
{
    internal class CassandraStatements
    {
        private readonly CassandraJournalConfig _config;
        private readonly string _createEventsByTagMaterializedView;

        public CassandraStatements(CassandraJournalConfig config)
        {
            _config = config;
            var tableName = $"{config.Keyspace}.{config.Table}";
            var configTableName = $"{config.Keyspace}.{config.ConfigTable}";
            var metadataTableName = $"{config.Keyspace}.{config.MetadataTable}";
            var eventsByTagViewName = $"{config.Keyspace}.{config.EventsByTagView}";

            CreateKeyspace =
                $@"
CREATE KEYSPACE IF NOT EXISTS {config.Keyspace}
WITH REPLICATION = {{ 'class' : {config
                    .ReplicationStrategy} }}
";

            CreateConfigTable =
                $@"
CREATE TABLE IF NOT EXISTS {configTableName} (
    property text primary key, value text)
";

            // message is the serialized Persistent that was used in v0.6 and earlier
            // event is the serialized application event that is used in v0.7 and later
            CreateTable =
                $@"
CREATE TABLE IF NOT EXISTS {tableName} (
    used boolean static,
    persistence_id text,
    partition_nr bigint,
    sequence_nr bigint,
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
    PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp, timebucket))
    WITH gc_grace_seconds ={config
                    .GcGraceSeconds}
    AND compaction = {config.TableCompactionStrategy.AsCQL}
";

            CreateMetadataTable =
                $@"
CREATE TABLE IF NOT EXISTS {metadataTableName}(
    persistence_id text PRIMARY KEY,
    deleted_to bigint,
    properties map<text,text>)
";

            _createEventsByTagMaterializedView =
                $@"
CREATE MATERIALIZED VIEW IF NOT EXISTS {eventsByTagViewName}{{0}} AS
    SELECT tag{{0}}, timebucket, timestamp, persistence_id, partition_nr, sequence_nr, writer_uuid, ser_id, ser_manifest, event_manifest, event, message
    FROM {tableName}
    WHERE persistence_id IS NOT NULL AND partition_nr IS NOT NULL AND sequence_nr IS NOT NULL
    AND tag{{0}} IS NOT NULL AND timestamp IS NOT NULL AND timebucket IS NOT NULL
    PRIMARY KEY ((tag{{0}}, timebucket), timestamp, persistence_id, partition_nr, sequence_nr)
    WITH CLUSTERING ORDER BY (timestamp ASC)
";

            WriteMessage =
                $@"
INSERT INTO {tableName} (persistence_id, partition_nr, sequence_nr, timestamp, timebucket, writer_uuid, ser_id, ser_manifest, event_manifest, event, tag1, tag2, tag3, message, used)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , true)
";

            DeleteMessage =
                $@"
DELETE FROM {tableName} WHERE
    persistence_id = ? AND
    partition_nr = ? AND
    sequence_nr = ?
";

            DeleteMessages = config.Cassandra2XCompat
                ? $@"
DELETE FROM {tableName} WHERE
    persistence_id = ? AND
    partition_nr = ? AND
    sequence_nr = ?
"
                : $@"
DELETE FROM {tableName} WHERE
    persistence_id = ? AND
    partition_nr = ? AND
    sequence_nr <= ?
";

            SelectMessages =
                $@"
SELECT * FROM {tableName} WHERE
    persistence_id = ? AND
    partition_nr = ? AND
    sequence_nr >= ? AND
    sequence_nr <= ?
";

            SelectInUse =
                $@"
SELECT used from {tableName} WHERE
    persistence_id = ? AND
    partition_nr = ?
";

            SelectConfig =
                $@"
SELECT * FROM {configTableName}
";

            WriteConfig =
                $@"
INSERT INTO {configTableName}(property, value) VALUES(?, ?) IF NOT EXISTS
";

            SelectHighestSequenceNr =
                $@"
SELECT sequence_nr, used FROM {tableName} WHERE
    persistence_id = ? AND
    partition_nr = ?
    ORDER BY sequence_nr
    DESC LIMIT 1
";

            SelectDeletedTo =
                $@"
SELECT deleted_to FROM {metadataTableName} WHERE
    persistence_id = ?
";

            InsertDeletedTo =
                $@"
INSERT INTO {metadataTableName} (persistence_id, deleted_to)
VALUES ( ?, ? )
";

            WriteInUse =
                $@"
INSERT INTO {tableName} (persistence_id, partition_nr, used)
VALUES(?, ?, true)
";
        }

        public string CreateKeyspace { get; }

        public string CreateConfigTable { get; }

        public string CreateTable { get; }

        public string CreateMetadataTable { get; }

        public string CreateEventsByTagMaterializedView(int tagId)
            => string.Format(_createEventsByTagMaterializedView, tagId);

        public string WriteMessage { get; }

        public string DeleteMessage { get; }

        public string DeleteMessages { get; }

        public string SelectMessages { get; }

        public string SelectInUse { get; }

        public string SelectConfig { get; }

        public string WriteConfig { get; }

        public string SelectHighestSequenceNr { get; }

        public string SelectDeletedTo { get; }

        public string InsertDeletedTo { get; }

        public string WriteInUse { get; }

        /// <summary>
        /// <para>
        /// Execute creation of keyspace and tables is limited to one thread at a time
        /// to reduce the risk of (annoying) "Column family ID mismatch" exception
        /// when write and read-side plugins are started at the same time.
        /// Those statements are retried, because that could happen across different
        /// nodes also but serializing those statements gives a better "experience".
        /// </para>
        /// <para>
        /// The materialized view for eventsByTag query is not created if <paramref name="maxTagId"/> is 0.
        /// </para>
        /// </summary>
        public Task ExecuteCreateKeyspaceAndTables(ISession session, CassandraJournalConfig config, int maxTagId)
        {
            return CassandraSession.SerializedExecution(
                () => ExecuteCreateKeyspaceAndTables(session, config, maxTagId),
                () => Create(session, config, maxTagId)
                );
        }

        private Task Create(ISession session, CassandraJournalConfig config, int maxTagId)
        {
            var keyspace = config.KeyspaceAutocreate
                ? (Task) session.ExecuteAsync(new SimpleStatement(CreateKeyspace))
                : Task.FromResult(new object());

            var tables =
                config.TablesAutocreate
                    ? Task.WhenAll(
                        keyspace,
                        session.ExecuteAsync(new SimpleStatement(CreateTable)),
                        session.ExecuteAsync(new SimpleStatement(CreateMetadataTable)),
                        session.ExecuteAsync(new SimpleStatement(CreateConfigTable))
                        )
                    : keyspace;

            if (config.TablesAutocreate)
            {
                var createTagStatements = Enumerable.Range(1, maxTagId).Select(CreateEventsByTagMaterializedView);
                return createTagStatements.Aggregate(tables, (task, statement) =>
                {
                    return task
                        .OnRanToCompletion(() => session.ExecuteAsync(new SimpleStatement(statement)))
                        .Unwrap();
                });
            }
            return tables;
        }

        public Task<ImmutableDictionary<string, string>> InitializePersistentConfig(ISession session)
        {
            return session.ExecuteAsync(new SimpleStatement(SelectConfig))
                .OnRanToCompletion(rs =>
                {
                    var properties = rs.ToList()
                        .ToImmutableDictionary(row => row.GetValue<string>("property"),
                            row => row.GetValue<string>("value"));
                    string oldValue;
                    if (properties.TryGetValue(CassandraJournalConfig.TargetPartitionProperty, out oldValue))
                    {
                        AssertCorrectPartitionSize(oldValue);
                        return Task.FromResult(properties);
                    }
                    return session.ExecuteAsync(new SimpleStatement(WriteConfig,
                        CassandraJournalConfig.TargetPartitionProperty, _config.TargetPartitionSize.ToString()))
                        .OnRanToCompletion(_ => properties.SetItem(CassandraJournalConfig.TargetPartitionProperty,
                                    _config.TargetPartitionSize.ToString()));
                })
                .Unwrap();
        }

        // ReSharper disable once UnusedParameter.Local
        private void AssertCorrectPartitionSize(string size)
        {
            if (int.Parse(size) != _config.TargetPartitionSize)
                throw new ArgumentException("Can't change target-partition-size");
        }
    }
}
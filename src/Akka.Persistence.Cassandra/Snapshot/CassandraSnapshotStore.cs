//-----------------------------------------------------------------------
// <copyright file="CassandraSnapshotStore.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using Akka.Util.Internal;
using Cassandra;

namespace Akka.Persistence.Cassandra.Snapshot
{
    /// <summary>
    /// A SnapshotStore implementation for writing snapshots to Cassandra.
    /// </summary>
    public class CassandraSnapshotStore : SnapshotStore
    {
        private readonly CassandraSnapshotStoreConfig _config;
        private readonly Akka.Serialization.Serialization _serialization;
        private readonly Serializer _serializer;

        private readonly CassandraSession _session;
        private readonly Lazy<Task<PreparedStatement>> _preparedWriteSnapshot;
        private readonly Lazy<Task<PreparedStatement>> _preparedDeleteSnapshot;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectSnapshot;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectSnapshotMetadataForLoad;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectSnapshotMetadataForDelete;

        private readonly Lazy<Information> _transportInformation;

        private readonly ILoggingAdapter _log;

        public CassandraSnapshotStore(Config cfg)
        {
            _config = new CassandraSnapshotStoreConfig(Context.System, cfg);
            _serialization = Context.System.Serialization;
            _serializer = Context.System.Serialization.FindSerializerForType(typeof(Serialization.Snapshot));

            _log = Context.GetLogger();
            var statements = new CassandraStatements(_config);
            _session = new CassandraSession(Context.System, _config, _log, Self.Path.Name,
                session => statements.ExecuteCreateKeyspaceAndTables(session, _config));

            _preparedWriteSnapshot =
                new Lazy<Task<PreparedStatement>>(() => Prepare(statements.WriteSnapshot, _config.WriteConsistency));
            _preparedDeleteSnapshot =
                new Lazy<Task<PreparedStatement>>(() => Prepare(statements.DeleteSnapshot, _config.WriteConsistency));
            _preparedSelectSnapshot =
                new Lazy<Task<PreparedStatement>>(() => Prepare(statements.SelectSnapshot, _config.ReadConsistency));
            _preparedSelectSnapshotMetadataForLoad =
                new Lazy<Task<PreparedStatement>>(
                    () =>
                        Prepare(statements.SelectSnapshotMetadata(_config.MaxMetadataResultSize),
                            _config.ReadConsistency));
            _preparedSelectSnapshotMetadataForDelete =
                new Lazy<Task<PreparedStatement>>(
                    () => Prepare(statements.SelectSnapshotMetadata(), _config.ReadConsistency));

            var address = ((ExtendedActorSystem) Context.System).Provider.DefaultAddress;
            _transportInformation =
                new Lazy<Information>(
                    () =>
                        !string.IsNullOrEmpty(address.Host)
                            ? new Information {Address = address, System = Context.System}
                            : null);
        }


        private async Task<PreparedStatement> Prepare(string statement, ConsistencyLevel consistencyLevel)
        {
            var preparedStatement = await _session.Prepare(statement);
            return preparedStatement.SetConsistencyLevel(consistencyLevel);
        }

        protected override void PreStart()
        {
            // eager initialization, but not from constructor
            Self.Tell(Init.Instance, Self);
        }

        protected override bool ReceivePluginInternal(object message)
        {
            if (message is Init)
            {
                // try initialize early, to be prepared for first real request
                // ReSharper disable NotAccessedVariable, RedundantAssignment
                var _ = _preparedWriteSnapshot.Value;
                _ = _preparedDeleteSnapshot.Value;
                _ = _preparedSelectSnapshot.Value;
                _ = _preparedSelectSnapshotMetadataForLoad.Value;
                _ = _preparedSelectSnapshotMetadataForDelete.Value;
                // ReSharper enable NotAccessedVariable, RedundantAssignment
                return true;
            }
            return false;
        }

        protected override void PostStop()
        {
            _session.Close();
            base.PostStop();
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var preparedStatement = await _preparedSelectSnapshotMetadataForLoad.Value;
            var metadata = await Metadata(preparedStatement, persistenceId, criteria, 3);
            var snapshot = await LoadNAsync(metadata);
            return snapshot;
        }

        private async Task<SelectedSnapshot> LoadNAsync(IEnumerable<SnapshotMetadata> metadata)
        {
            foreach (var md in metadata)
            {
                try
                {
                    var snapshot = await Load1Async(md);
                    var selectedSnapshot = new SelectedSnapshot(md, snapshot.Data);
                    return selectedSnapshot;
                }
                catch (Exception e)
                {
                    _log.Warning(
                        // ReSharper disable once PossibleNullReferenceException
                        $"Failed to load snapshot, trying older one. Caused by: {e.Message}");
                }
            }
            return null;
        }

        private async Task<Serialization.Snapshot> Load1Async(SnapshotMetadata metadata)
        {
            var preparedStatement = await _preparedSelectSnapshot.Value;
            var boundSelectSnapshot = preparedStatement.Bind(metadata.PersistenceId, metadata.SequenceNr);

            var resultSet = await _session.Select(boundSelectSnapshot);

            var row = resultSet.Single();
            var bytes = row.GetValue<byte[]>("snapshot");
            if (bytes == null)
            {
                return new Serialization.Snapshot(_serialization.Deserialize(
                    row.GetValue<byte[]>("snapshot_data"),
                    row.GetValue<int>("ser_id"),
                    row.GetValue<string>("ser_manifest")
                    ));
            }
            // for backwards compatibility
            return (Serialization.Snapshot) _serializer.FromBinary(bytes, typeof(Serialization.Snapshot));
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var serialized = Serialize(snapshot);
            var preparedStatement = await _preparedWriteSnapshot.Value;

            var boundStatement = preparedStatement.Bind(new
            {
                persistence_id = metadata.PersistenceId,
                sequence_nr = metadata.SequenceNr,
                timestamp = metadata.Timestamp.ToUniversalTime().Ticks,
                ser_id = serialized.SerializerId,
                ser_manifest = serialized.SerializationManifest,
                snapshot_data = serialized.Bytes,
                // for backwards compatibility
                snapshot = (string) null
            });
            await _session.ExecuteWrite(boundStatement);
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            var preparedStatement = await _preparedDeleteSnapshot.Value;
            var boundDeleteSnapshot = preparedStatement.Bind(metadata.PersistenceId, metadata.SequenceNr);
            await _session.ExecuteWrite(boundDeleteSnapshot);
        }

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var preparedStatement = await _preparedSelectSnapshotMetadataForDelete.Value;
            var metadata = await Metadata(preparedStatement, persistenceId, criteria);
            var boundStatements = await Task.WhenAll(metadata
                .Select(async m =>
                {
                    var ps = await _preparedDeleteSnapshot.Value;
                    return ps.Bind(m.PersistenceId, m.SequenceNr);
                }));
            await ExecuteBatch(batch => boundStatements.ForEach(s => batch.Add(s)));
        }

        private async Task ExecuteBatch(Action<BatchStatement> body)
        {
            var batch = (BatchStatement) new BatchStatement().SetConsistencyLevel(_config.WriteConsistency);
            body(batch);
            var session = await _session.Underlying;
            await session.ExecuteAsync(batch);
        }

        private Serialized Serialize(object payload)
        {
            // serialize actor references with full address information (defaultAddress)
            if (_transportInformation.Value != null)
                return Akka.Serialization.Serialization.SerializeWithTransport(_transportInformation.Value.System,
                    _transportInformation.Value.Address, () => SerializeSnapshot(payload));
            return SerializeSnapshot(payload);
        }

        private Serialized SerializeSnapshot(object payload)
        {
            var serializer = _serialization.FindSerializerFor(payload);
            string serializationManifest;
            if (serializer is SerializerWithStringManifest)
                serializationManifest = ((SerializerWithStringManifest) serializer).Manifest(payload);
            else if (serializer.IncludeManifest)
                serializationManifest = payload.GetType().ToQualifiedString();
            else
                serializationManifest = Persistent.Undefined;
            var serializedPayload = serializer.ToBinary(payload);
            return new Serialized(serializedPayload, serializationManifest, serializer.Identifier);
        }

        private Task<ICollection<SnapshotMetadata>> Metadata(PreparedStatement preparedStatement, string persistenceId,
            SnapshotSelectionCriteria criteria, int? limit = null)
        {
            // TODO the RowIterator is using some blocking, would benefit from a rewrite
            var promise = new TaskCompletionSource<ICollection<SnapshotMetadata>>();
            var result = 
                Rows(preparedStatement, persistenceId, criteria.MaxSequenceNr)
                .Where(row => row.GetValue<long>("timestamp") <= criteria.MaxTimeStamp.Ticks);
            if (limit.HasValue)
                result = result.Take(limit.Value);
            promise.SetResult(result.Select(MapRowToSnapshotMetadata).ToList());
            // TODO schedule on blockingDispatcher?

            return promise.Task;
        }

        private IEnumerable<Row> Rows(PreparedStatement preparedStatement, string persistenceId,
            long maxSequenceNr)
        {
            var currentSequenceNr = maxSequenceNr;
            var rowCount = 0;

            // we know that the session is initialized, since we got preparedStatement
            _session.Underlying.Wait(_config.BlockingTimeout);
            var session = _session.Underlying.Result;

            // FIXME more blocking
            var enumerator = session.Execute(preparedStatement.Bind(persistenceId, currentSequenceNr)).GetEnumerator();
            var hasNext = enumerator.MoveNext();
            while (hasNext)
            {
                var row = enumerator.Current;
                currentSequenceNr = row.GetValue<long>("sequence_nr");
                rowCount += 1;
                yield return row;
                hasNext = enumerator.MoveNext();
                if (!hasNext && rowCount >= _config.MaxMetadataResultSize)
                {
                    rowCount = 0;
                    currentSequenceNr -= 1;
                    enumerator = session.Execute(preparedStatement.Bind(persistenceId, currentSequenceNr)).GetEnumerator();
                    hasNext = enumerator.MoveNext();
                }
            }
        }

        private static SnapshotMetadata MapRowToSnapshotMetadata(Row row)
        {
            return new SnapshotMetadata(row.GetValue<string>("persistence_id"), row.GetValue<long>("sequence_nr"),
                                        new DateTime(row.GetValue<long>("timestamp"), DateTimeKind.Utc));
        }

        private class Init
        {
            public static readonly Init Instance = new Init();
            private Init() { }
        }

        private class Serialized
        {
            public byte[] Bytes{ get; }
            public string SerializationManifest { get; }
            public int SerializerId { get; }

            public Serialized(byte[] bytes, string serializationManifest, int serializerId)
            {
                Bytes = bytes;
                SerializationManifest = serializationManifest;
                SerializerId = serializerId;
            }
        }
    }
}


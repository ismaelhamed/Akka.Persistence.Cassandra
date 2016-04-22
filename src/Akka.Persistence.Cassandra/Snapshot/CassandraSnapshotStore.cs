using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
                new Lazy<Task<PreparedStatement>>(
                    () =>
                        _session.Prepare(statements.WriteSnapshot)
                            .ContinueWith(t => t.Result.SetConsistencyLevel(_config.WriteConsistency),
                                TaskContinuationOptions.OnlyOnRanToCompletion));
            _preparedDeleteSnapshot =
                new Lazy<Task<PreparedStatement>>(
                    () =>
                        _session.Prepare(statements.DeleteSnapshot)
                            .ContinueWith(t => t.Result.SetConsistencyLevel(_config.WriteConsistency),
                                TaskContinuationOptions.OnlyOnRanToCompletion));
            _preparedSelectSnapshot =
                new Lazy<Task<PreparedStatement>>(
                    () =>
                        _session.Prepare(statements.SelectSnapshot)
                            .ContinueWith(t => t.Result.SetConsistencyLevel(_config.ReadConsistency),
                                TaskContinuationOptions.OnlyOnRanToCompletion));
            _preparedSelectSnapshotMetadataForLoad =
                new Lazy<Task<PreparedStatement>>(
                    () =>
                        _session.Prepare(statements.SelectSnapshotMetadata(_config.MaxMetadataResultSize))
                            .ContinueWith(t => t.Result.SetConsistencyLevel(_config.ReadConsistency),
                                TaskContinuationOptions.OnlyOnRanToCompletion));
            _preparedSelectSnapshotMetadataForDelete =
                new Lazy<Task<PreparedStatement>>(
                    () =>
                        _session.Prepare(statements.SelectSnapshotMetadata())
                            .ContinueWith(t => t.Result.SetConsistencyLevel(_config.ReadConsistency),
                                TaskContinuationOptions.OnlyOnRanToCompletion));

            var address = ((ExtendedActorSystem) Context.System).Provider.DefaultAddress;
            _transportInformation =
                new Lazy<Information>(
                    () =>
                        !string.IsNullOrEmpty(address.Host)
                            ? new Information {Address = address, System = Context.System}
                            : null);
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

        protected override Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            return _preparedSelectSnapshotMetadataForLoad.Value
                .ContinueWith(t => Metadata(t.Result, persistenceId, criteria, 3),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap()
                .ContinueWith(t => LoadNAsync(t.Result.ToImmutableList()), TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap();
        }

        private Task<SelectedSnapshot> LoadNAsync(ImmutableList<SnapshotMetadata> metadata)
        {
            if (metadata.Count == 0)
                return Task.FromResult((SelectedSnapshot) null);
            var md = metadata[0];

            return Load1Async(md)
                .ContinueWith(t => new SelectedSnapshot(md, t.Result.Data),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                .ContinueWith(t =>
                {
                    _log.Warning(
                        // ReSharper disable once PossibleNullReferenceException
                        $"Failed to load snapshot, trying older one. Caused by: {(t.IsFaulted ? t.Exception.Message : "Cancelled")}");
                    return LoadNAsync(metadata.RemoveAt(0));
                }, TaskContinuationOptions.NotOnRanToCompletion)
                .Unwrap();
        }

        private Task<Serialization.Snapshot> Load1Async(SnapshotMetadata metadata)
        {
            var boundSelectSnapshot = _preparedSelectSnapshot.Value
                .ContinueWith(t => t.Result.Bind(metadata.PersistenceId, metadata.SequenceNr),
                    TaskContinuationOptions.OnlyOnRanToCompletion);

            return boundSelectSnapshot
                .ContinueWith(t => _session.Select(t.Result), TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap()
                .ContinueWith(t =>
                {
                    var row = t.Result.Single();
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
                });
        }

        protected override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var serialized = Serialize(snapshot);
            return _preparedWriteSnapshot.Value
                .ContinueWith(t =>
                {
                    var bs = t.Result.Bind(new
                    {
                        persistence_id = metadata.PersistenceId,
                        sequence_nr = metadata.SequenceNr,
                        timestamp = metadata.Timestamp,
                        ser_id = serialized.SerializerId,
                        ser_manifest = serialized.SerializationManifest,
                        snapshot_data = serialized.Bytes,
                        // for backwards compatibility
                        snapshot = (string) null
                    });
                    return _session.ExecuteWrite(bs);
                }, TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap();
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            var boundDeleteSnapshot = _preparedDeleteSnapshot.Value
                .ContinueWith(t => t.Result.Bind(metadata.PersistenceId, metadata.SequenceNr),
                    TaskContinuationOptions.OnlyOnRanToCompletion);
            return boundDeleteSnapshot
                .ContinueWith(t => _session.ExecuteWrite(t.Result), TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap();
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            return _preparedSelectSnapshotMetadataForDelete.Value
                .ContinueWith(preparedStatementTask => Metadata(preparedStatementTask.Result, persistenceId, criteria),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap()
                .ContinueWith(metadataTask =>
                {
                    var boundStatements = metadataTask.Result
                        .Select(metadata => _preparedDeleteSnapshot.Value
                            .ContinueWith(
                                _ =>
                                    _.Result.Bind(metadata.PersistenceId, metadata.SequenceNr,
                                        TaskContinuationOptions.OnlyOnRanToCompletion)
                            ));
                    return Task.WhenAll(boundStatements);
                }, TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap()
                .ContinueWith(
                    statementsTask => ExecuteBatch(batch => statementsTask.Result.ForEach(s => batch.Add(s))),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap();
        }

        private Task ExecuteBatch(Action<BatchStatement> body)
        {
            var batch = (BatchStatement) new BatchStatement().SetConsistencyLevel(_config.WriteConsistency);
            body(batch);
            return _session.Underlying
                .ContinueWith(_ => _.Result.ExecuteAsync(batch), TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap();
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
                .Where(row => row.GetValue<long>("timestamp") > criteria.MaxTimeStamp.Ticks);
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
                                        new DateTime(row.GetValue<long>("timestamp")));
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


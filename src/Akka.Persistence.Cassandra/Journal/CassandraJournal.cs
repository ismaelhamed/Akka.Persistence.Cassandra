//-----------------------------------------------------------------------
// <copyright file="CassandraJournal.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Configuration;
using Akka.Dispatch;
using Akka.Event;
using Akka.Persistence.Cassandra.Query;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Serialization;
using Akka.Streams;
using Akka.Util.Internal;
using Cassandra;

namespace Akka.Persistence.Cassandra.Journal
{
    /// <summary>
    /// An Akka.NET journal implementation that writes events asynchronously to Cassandra.
    /// </summary>
    public partial class CassandraJournal : AsyncWriteJournal
    {
        #region Internal classes

        private sealed class Init
        {
            public static readonly Init Instance = new Init();
            private Init() { }
        }

        private sealed class WriteFinished : INoSerializationVerificationNeeded
        {
            public WriteFinished(string persistenceId, Task task)
            {
                PersistenceId = persistenceId;
                Task = task;
            }

            public string PersistenceId { get; }
            public Task Task { get; }
        }

        private sealed class SerializedAtomicWrite
        {
            public SerializedAtomicWrite(string persistenceId, ImmutableList<Serialized> payload)
            {
                PersistenceId = persistenceId;
                Payload = payload;
            }

            public string PersistenceId { get; }
            public ImmutableList<Serialized> Payload { get; }
        }

        private sealed class Serialized
        {
            public Serialized(string persistenceId, long sequenceNr, byte[] data, IImmutableSet<string> tags, string eventManifest, string serializationManifest, int seralizerId, string writerGuid)
            {
                PersistenceId = persistenceId;
                SequenceNr = sequenceNr;
                Data = data;
                Tags = tags;
                EventManifest = eventManifest;
                SerializationManifest = serializationManifest;
                SeralizerId = seralizerId;
                WriterGuid = writerGuid;
            }

            public string PersistenceId { get; }
            public long SequenceNr { get; }
            public byte[] Data { get; }
            public IImmutableSet<string> Tags { get; }
            public string EventManifest { get; }
            public string SerializationManifest { get; }
            public int SeralizerId { get; }
            public string WriterGuid { get; }
        }

        private sealed class PartitionInfo
        {
            public PartitionInfo(long partitionNr, long minSequenceNr, long maxSequenceNr)
            {
                PartitionNr = partitionNr;
                MinSequenceNr = minSequenceNr;
                MaxSequenceNr = maxSequenceNr;
            }

            public long PartitionNr { get; }
            public long MinSequenceNr { get; }
            public long MaxSequenceNr { get; }
        }

        #endregion

        private readonly CassandraJournalConfig _config;
        private readonly Akka.Serialization.Serialization _serialization;
        private readonly MessageDispatcher _blockingDispatcher;
        private readonly IDictionary<string, Task> _writeInProgress;
        private readonly IActorRef _pubsub;
        private readonly CassandraSession _session;

        private readonly Lazy<Task<PreparedStatement>> _preparedWriteMessage;
        private readonly Lazy<Task<PreparedStatement>> _preparedDeleteMessages;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectMessages;
        private readonly Lazy<Task<PreparedStatement>> _preparedCheckInUse;
        private readonly Lazy<Task<PreparedStatement>> _preparedWriteInUse;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectHighestSequenceNr;
        private readonly Lazy<Task<PreparedStatement>> _preparedSelectDeletedTo;
        private readonly Lazy<Task<PreparedStatement>> _preparedInsertDeletedTo;

        private readonly IRetryPolicy _writeRetryPolicy;
        private readonly IRetryPolicy _deleteRetryPolicy;

        private readonly Lazy<Information> _transportInformation;

        private readonly ILoggingAdapter _log = Context.GetLogger();

        public CassandraJournal(Config config)
        {
            _config = new CassandraJournalConfig(Context.System, config);
            _serialization = Context.System.Serialization;

            _blockingDispatcher = Context.System.Dispatchers.Lookup(_config.BlockingDispatcherId);

           // ReadHighestSequenceNr must be performed after pending write for a persistenceId
           // when the persistent actor is restarted.
           // It seems like Cassandra doesn't support session consistency so we handle it ourselves.
           // https://aphyr.com/posts/299-the-trouble-with-timestamps
           _writeInProgress = new Dictionary<string, Task>();

            // Announce written changes to DistributedPubSub if pubsub-minimum-interval is defined in config
            _pubsub = null;
            if (_config.PubsubMinimumInterval.HasValue &&
                _config.PubsubMinimumInterval.Value != Timeout.InfiniteTimeSpan)
            {
                // PubSub will be ignored when clustering is unavailable
                try
                {
                    var extension = DistributedPubSub.Get(Context.System);
                    if (!extension.IsTerminated)
                        _pubsub = Context.ActorOf(PubSubThrottler.Props(extension.Mediator,
                            _config.PubsubMinimumInterval.Value).WithDispatcher(Context.Props.Dispatcher));
                }
                catch
                {
                    // ignored
                }
            }

            var statements = new CassandraStatements(_config);
            _session = new CassandraSession(Context.System, _config, _log, Self.Path.Name,
                session => statements.ExecuteCreateKeyspaceAndTables(session, _config, _config.MaxTagId)
                    .OnRanToCompletion(() => statements.InitializePersistentConfig(session)).Unwrap());

            _preparedWriteMessage = new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.WriteMessage));
            _preparedDeleteMessages = new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.DeleteMessages));
            _preparedSelectMessages =
                new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.SelectMessages)
                    .OnRanToCompletion(ps => ps.SetConsistencyLevel(_config.ReadConsistency)));
            _preparedCheckInUse =
                new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.SelectInUse)
                    .OnRanToCompletion(ps => ps.SetConsistencyLevel(_config.ReadConsistency)));
            _preparedWriteInUse = new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.WriteInUse));
            _preparedSelectHighestSequenceNr =
                new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.SelectHighestSequenceNr)
                    .OnRanToCompletion(ps => ps.SetConsistencyLevel(_config.ReadConsistency)));
            _preparedSelectDeletedTo =
                new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.SelectDeletedTo)
                    .OnRanToCompletion(ps => ps.SetConsistencyLevel(_config.ReadConsistency)));
            _preparedInsertDeletedTo =
                new Lazy<Task<PreparedStatement>>(() => _session.Prepare(statements.InsertDeletedTo)
                    .OnRanToCompletion(ps => ps.SetConsistencyLevel(_config.WriteConsistency)));

            _writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(_config.WriteRetries));
            _deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(_config.DeleteRetries));

            var address = ((ExtendedActorSystem) Context.System).Provider.DefaultAddress;
            _transportInformation =
                new Lazy<Information>(
                    () =>
                        !string.IsNullOrEmpty(address.Host)
                            ? new Information {Address = address, System = Context.System}
                            : null);

            _materializer = ActorMaterializer.Create(Context);
            _queries = PersistenceQuery.Get(Context.System).ReadJournalFor<CassandraReadJournal>(_config.QueryPlugin);
        }
        
        protected override void PreStart()
        {
            // eager initialization, but not from constructor
            Self.Tell(Init.Instance);
        }

        protected override bool ReceivePluginInternal(object message)
        {
            if (message is WriteFinished)
            {
                var writeFinished = (WriteFinished) message;
                Task current;
                if (_writeInProgress.TryGetValue(writeFinished.PersistenceId, out current) &&
                    current == writeFinished.Task)
                    _writeInProgress.Remove(writeFinished.PersistenceId);
            }
            else if (message is Init)
            {
                // try initialize early, to be prepared for first real request
                // ReSharper disable RedundantAssignment
                // ReSharper disable once NotAccessedVariable
                var _ = _preparedWriteMessage.Value;
                _ = _preparedDeleteMessages.Value;
                _ = _preparedSelectMessages.Value;
                _ = _preparedCheckInUse.Value;
                _ = _preparedWriteInUse.Value;
                _ = _preparedSelectHighestSequenceNr.Value;
                _ = _preparedSelectDeletedTo.Value;
                _ = _preparedInsertDeletedTo.Value;
                // ReSharper enable RedundantAssignment
            }
            else return false;
            return true;
        }

        protected override void PostStop()
        {
            _session.Close();
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            // We need to preserve the order / size of this sequence even though we don't map
            // AtomicWrites 1:1 with a Cassandra insert
            //
            // We must NOT catch serialization exceptions here because rejections will cause
            // holes in the sequence number series and we use the sequence numbers to detect
            // missing (delayed) events in the eventByTag query.
            //
            // Note that we assume that all messages have the same persistenceId, which is
            // the case for Akka 2.4.2.
            
            var promise = new TaskCompletionSource<object>();
            var self = Self;
            var messageList = messages.ToList();
            var persistenceId = messageList.First().PersistenceId;
            _writeInProgress[persistenceId] = promise.Task;

            IList<SerializedAtomicWrite> serialized;
            try
            {
                serialized = messageList.Select(Serialize).ToList();
            }
            catch (Exception e)
            {
                var failure = new TaskCompletionSource<IImmutableList<Exception>>();
                failure.SetException(e);
                return await failure.Task;
            }

            Task result = null;
            try
            {
                if (messageList.Count <= _config.MaxMessageBatchSize)
                {
                    // optimize for the common case
                    result = WriteMessages(serialized);
                    await result;
                }
                else
                {
                    var position = 0;
                    while (position < serialized.Count)
                    {
                        await WriteMessages(serialized.Skip(position).Take(_config.MaxMessageBatchSize).ToList());
                        position += _config.MaxMessageBatchSize;
                    }
                }

            }
            finally
            {
                self.Tell(new WriteFinished(persistenceId, promise.Task));
                promise.SetResult(new object());
            }

            PublishTagNotification(serialized, result);
            // null == all good
            return null;
        }

        private SerializedAtomicWrite Serialize(AtomicWrite aw)
        {
            return new SerializedAtomicWrite(
                aw.PersistenceId,
                ((IEnumerable<IPersistentRepresentation>)aw.Payload).Select(p =>
                {
                    if (!(p.Payload is Tagged)) return SerializeEvent(p, ImmutableHashSet<string>.Empty);
                    var tagged = (Tagged) p.Payload;
                    var taggedPayload = p.WithPayload(tagged.Payload);
                    return SerializeEvent(taggedPayload, tagged.Tags);
                }).ToImmutableList()
            );
        }

        private void PublishTagNotification(IEnumerable<SerializedAtomicWrite> serialized, Task result)
        {
            if (_pubsub != null)
            {
                result
                    .OnRanToCompletion(() =>
                    {
                        foreach (var tag in serialized.SelectMany(s => s.Payload.SelectMany(p => p.Tags)).Distinct())
                        {
                            _pubsub.Tell(new Publish("akka.persistence.cassandra.journal.tag", tag));
                        }
                    });
            }
        }

        private async Task WriteMessages(IList<SerializedAtomicWrite> atomicWrites)
        {
            var boundStatements = StatementGroup(atomicWrites);
            if (boundStatements.Count == 1)
            {
                var statement = await boundStatements[0];
                await Execute(statement, _writeRetryPolicy);
            }
            else if (boundStatements.Count == 0)
            {
                await Task.FromResult(new object());
            }
            else
            {
                var statements = await Task.WhenAll(boundStatements);
                await ExecuteBatch(batch => statements.ForEach(s => batch.Add(s)), _writeRetryPolicy);
            }
        }

        private IList<Task<BoundStatement>> StatementGroup(IList<SerializedAtomicWrite> atomicWrites)
        {
            var lastPayload = atomicWrites[atomicWrites.Count - 1].Payload;
            var maxPartitionNr = PartitionNr(lastPayload[lastPayload.Count - 1].SequenceNr);
            var firstSequenceNr = atomicWrites[0].Payload[0].SequenceNr;
            var minPartitionNr = PartitionNr(firstSequenceNr);
            var persistenceId = atomicWrites[0].PersistenceId;
            var all = atomicWrites.SelectMany(aw => aw.Payload);


            // reading assumes sequence numbers are in the right partition or partition + 1
            // even if we did allow this it would perform terribly as large Cassandra batches are not good
            if (maxPartitionNr - minPartitionNr > 2)
                throw new ArgumentException(
                    "Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.");

            var writes = all.Select(async m =>
            {
                // use same clock source as the GUID for the timeBucket
                var nowGuid = TimeUuid.NewId();
                var now = nowGuid.GetDate();
                var statement = await _preparedWriteMessage.Value;
                var parameters = new object[]
                {
                    persistenceId, // persistence_id
                    maxPartitionNr, // partition_nr
                    m.SequenceNr, // sequence_nr
                    nowGuid, // timestamp
                    new TimeBucket(now).Key, // timebucket
                    m.WriterGuid, // writer_uuid
                    m.SeralizerId, // ser_id
                    m.SerializationManifest, // ser_manifest
                    m.EventManifest, // event_manifest
                    m.Data, // event
                    null, // tag1
                    null, // tag2
                    null, // tag3
                    // for backwards compatibility
                    null // message
                };
                if (m.Tags.Count > 0)
                {
                    var tagCounts = new int[_config.MaxTagsPerEvent];
                    foreach (var tag in m.Tags)
                    {
                        int tagId;
                        if (!_config.Tags.TryGetValue(tag, out tagId))
                            tagId = 1;
                        parameters[9 + tagId] = tag;
                        tagCounts[tagId - 1] = tagCounts[tagId - 1] + 1;
                        for (var i = 0; i < tagCounts.Length; i++)
                        {
                            if (tagCounts[i] <= 1) continue;
                            if (_log.IsWarningEnabled)
                                _log.Warning(
                                    $"Duplicate tag identifier [{i + 1}] among tags [{string.Join(",", m.Tags)}] for event from [{persistenceId}]. " +
                                    "Define known tags in cassandra-journal.tags configuration when using more than one tag per event.");
                        }
                    }
                }
                return (BoundStatement) statement.Bind(parameters);
            }).ToList();

            // in case we skip an entire partition we want to make sure the empty partition has an in-use flag so scans
            // keep going when they encounter it
            if (IsStartOfPartition(firstSequenceNr) && minPartitionNr != maxPartitionNr)
            {
                writes.Add(WriteInUse(persistenceId, minPartitionNr));
            }
            return writes;
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var highestDeletedSequenceNr = await HighestDeletedSequenceNrAsync(persistenceId);
            var lowestSequenceNr = await ReadLowestSequenceNrAsync(persistenceId, 1L, highestDeletedSequenceNr);
            var highestSequenceNr = await ReadHighestSequenceNrAsync(persistenceId, lowestSequenceNr);
            var lowestPartition = PartitionNr(lowestSequenceNr);
            var toSeqNr = Math.Min(toSequenceNr, highestSequenceNr);
            var highestPartition = PartitionNr(toSeqNr) + 1; // may have been moved to the next partition

            var preparedStatement = await _preparedInsertDeletedTo.Value;
            var boundInsertDeletedTo = preparedStatement.Bind(persistenceId, toSeqNr);
            var logicalDelete = _session.ExecuteWrite(boundInsertDeletedTo);

            var partitionInfos = new List<Task<PartitionInfo>>();
            for (var n = lowestPartition; n <= highestPartition; n++)
                partitionInfos.Add(PartitionInfoAsync(persistenceId, n, toSeqNr));

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            logicalDelete.ContinueWith(_ => PhysicalDelete(persistenceId, toSequenceNr, partitionInfos));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            await logicalDelete;
        }

        private async Task PhysicalDelete(string persistenceId, long toSequenceNr,
            IEnumerable<Task<PartitionInfo>> partitionInfos)
        {
            if (_config.Cassandra2XCompat)
            {
                foreach (var partitionInfoTask in partitionInfos)
                {
                    var partitionInfo = await partitionInfoTask;
                    var fromSeqNr = partitionInfo.MinSequenceNr;
                    while (fromSeqNr <= partitionInfo.MaxSequenceNr)
                    {
                        var toSeqNr = Math.Min(fromSeqNr + _config.MaxMessageBatchSize - 1,
                            partitionInfo.MaxSequenceNr);
                        var delete = DeleteMessagesCassandra2XAsync(persistenceId, partitionInfo.PartitionNr, fromSeqNr,
                            toSeqNr);
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                        delete.OnFaultedOrCanceled(t =>
                        {
                            if (_log.IsWarningEnabled)
                                _log.Warning(
                                    $"Unable to complete deletes for persistence id {persistenceId}, toSequenceNr ${toSequenceNr}." +
                                    "The plugin will continue to function correctly but you will need to manually delete the old messages.",
                                    t.IsFaulted ? t.Exception.Unwrap() : new OperationCanceledException());

                        });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                        fromSeqNr = toSeqNr + 1;
                    }
                }
            }
            else
            {
                var preparedStatement = await _preparedDeleteMessages.Value;
                var deletes = partitionInfos.Select(async partitionInfoTask =>
                {
                    var partitionInfo = await partitionInfoTask;
                    var boundDeleteMessages = preparedStatement.Bind(persistenceId, partitionInfo.PartitionNr,
                        toSequenceNr);
                    return Execute(boundDeleteMessages, _deleteRetryPolicy);
                });
                var all = Task.WhenAll(deletes);
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                all.OnFaultedOrCanceled(t =>
                {
                    if (_log.IsWarningEnabled)
                        _log.Warning(
                            $"Unable to complete deletes for persistence id {persistenceId}, toSequenceNr ${toSequenceNr}." +
                            "The plugin will continue to function correctly but you will need to manually delete the old messages.",
                            t.IsFaulted ? t.Exception.Unwrap() : new OperationCanceledException());

                });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            }
        }

        private async Task DeleteMessagesCassandra2XAsync(string persistenceId, long partitionNr, long fromSequenceNr, long toSequenceNr)
        {
            var preparedStatement = await _preparedDeleteMessages.Value;
            await ExecuteBatch(batch =>
            {
                for (var n = fromSequenceNr; n <= toSequenceNr; n++)
                    batch.Add(preparedStatement.Bind(persistenceId, partitionNr, n));
            }, _deleteRetryPolicy);
        }

        private async Task<BoundStatement> WriteInUse(string persistenceId, long partitionNr)
        {
            var statement = await _preparedWriteInUse.Value;
            return statement.Bind(persistenceId, partitionNr);
        }

        private long PartitionNr(long sequenceNr)
        {
            return (sequenceNr - 1L)/_config.TargetPartitionSize;
        }

        private async Task<PartitionInfo> PartitionInfoAsync(string persistenceId, long partitionNr, long maxSequenceNr)
        {
            var preparedStatement = await _preparedSelectHighestSequenceNr.Value;
            var boundSelectHighestSequenceNr = preparedStatement.Bind(persistenceId, partitionNr);
            var rs = await _session.Select(boundSelectHighestSequenceNr);
            var row = rs.FirstOrDefault();
            return row != null
                ? new PartitionInfo(partitionNr, MinSequenceNr(partitionNr),
                    Math.Min(row.GetValue<long>("sequence_nr"), maxSequenceNr))
                : new PartitionInfo(partitionNr, MinSequenceNr(partitionNr), -1);
        }

        private async Task ExecuteBatch(Action<BatchStatement> body, IRetryPolicy retryPolicy)
        {
            var batch =
                (BatchStatement)
                    new BatchStatement().SetConsistencyLevel(_config.WriteConsistency).SetRetryPolicy(retryPolicy);
            body(batch);
            var session = await _session.Underlying;
            await session.ExecuteAsync(batch);
        }

        private async Task Execute(Statement statement, IRetryPolicy retryPolicy)
        {
            statement.SetConsistencyLevel(_config.WriteConsistency).SetRetryPolicy(retryPolicy);
            await _session.ExecuteWrite(statement);
        }


        private Task<long> ReadLowestSequenceNrAsync(string persistenceId, long fromSequenceNr,
            long highestDeletedSequenceNumber)
        {
            // TODO the RowEnumerator is using some blocking, would benefit from a rewrite
            var promise = new TaskCompletionSource<long>();
            try
            {
                using (var enumerator = new MessageEnumerator(this, persistenceId,
                    Math.Max(highestDeletedSequenceNumber + 1, fromSequenceNr)))
                {
                    long? sequenceNrOfFirstNonDeletedMessage = null;
                    while (enumerator.MoveNext())
                    {
                        if (!enumerator.Current.IsDeleted)
                        {
                            sequenceNrOfFirstNonDeletedMessage = enumerator.Current.SequenceNr;
                            break;
                        }
                    }
                    promise.SetResult(sequenceNrOfFirstNonDeletedMessage ?? fromSequenceNr);
                }
            }
            catch (Exception e)
            {
                promise.SetException(e);
            }
            return promise.Task;
        }

        private bool IsStartOfPartition(long sequenceNr) => (sequenceNr - 1L) % _config.TargetPartitionSize == 0L;

        private long MinSequenceNr(long partitionNr) => partitionNr*_config.TargetPartitionSize + 1;

        private Serialized SerializeEvent(IPersistentRepresentation payload, IImmutableSet<string> tags)
        {
            // serialize actor references with full address information (defaultAddress)
            if (_transportInformation.Value != null)
                return Akka.Serialization.Serialization.SerializeWithTransport(_transportInformation.Value.System,
                    _transportInformation.Value.Address, () => DoSerializeEvent(payload, tags));
            return DoSerializeEvent(payload, tags);
        }

        private Serialized DoSerializeEvent(IPersistentRepresentation payload, IImmutableSet<string> tags)
        {
            var @event = payload.Payload;
            var serializer = _serialization.FindSerializerFor(@event);
            string serializationManifest;
            if (serializer is SerializerWithStringManifest)
                serializationManifest = ((SerializerWithStringManifest) serializer).Manifest(@event);
            else if (serializer.IncludeManifest)
                serializationManifest = payload.GetType().ToQualifiedString();
            else
                serializationManifest = Persistent.Undefined;
            return new Serialized(
                payload.PersistenceId,
                payload.SequenceNr,
                serializer.ToBinary(@event),
                tags,
                payload.Manifest,
                serializationManifest,
                serializer.Identifier,
                payload.WriterGuid
            );
        }

        private class MessageEnumerator: IEnumerator<IPersistentRepresentation>
        {
            private CassandraJournal _journal;
            private readonly string _persistenceId;
            private readonly long _fromSequenceNr;
            private RowEnumerator _iter;

            public MessageEnumerator(CassandraJournal journal, string persistenceId, long fromSequenceNr)
            {
                _journal = journal;
                _persistenceId = persistenceId;
                _fromSequenceNr = fromSequenceNr;
                Reset();
            }

            public bool MoveNext()
            {
                var previous = Current;
                Current = null;
                while (_iter.MoveNext())
                {
                    var row = _iter.Current;
                    var sequenceNr = row.GetValue<long>("sequence_nr");
                    // there may be duplicates returned by row enumerator
                    // (on scan boundaries within a partition)
                    if (previous != null && sequenceNr == previous.SequenceNr) continue;
                    var messageBytes = row.GetValue<byte[]>("message");
                    if (messageBytes == null)
                    {
                        Current = new Persistent(
                            payload: DeserializeEvent(_journal._serialization, row),
                            sequenceNr: row.GetValue<long>("sequence_nr"),
                            persistenceId: row.GetValue<string>("persistence_id"),
                            manifest: row.GetValue<string>("event_manifest"),
                            isDeleted: false,
                            sender: null,
                            writerGuid: row.GetValue<string>("writer_uuid")
                            );
                    }
                    else
                    {
                        // for backwards compatibility
                        Current = PersistentFromBytes(messageBytes);
                    }
                }
                return Current != null;
            }

            public void Reset()
            {
                _iter = new RowEnumerator(_journal, _persistenceId, _fromSequenceNr);
            }

            public IPersistentRepresentation Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                _iter = null;
                _journal = null;
            }

            private Persistent PersistentFromBytes(byte[] bytes)
            {
                return
                    (Persistent)
                        _journal._serialization.FindSerializerForType(typeof(Persistent))
                            .FromBinary(bytes, typeof(Persistent));
            }
        }

        private class RowEnumerator: IEnumerator<Row>
        {
            private CassandraJournal _journal;
            private readonly string _persistenceId;
            private readonly long _fromSequenceNr;

            private readonly ISession _session;
            private readonly PreparedStatement _preparedSelectMessages;
            private readonly PreparedStatement _preparedCheckInUse;

            private long _currentPartitionNr;
            private long _currentSequenceNr;
            private long _startSequenceNr;
            private IEnumerator<Row> _rowEnumerator;

            public RowEnumerator(CassandraJournal journal, string persistenceId, long fromSequenceNr)
            {
                _journal = journal;
                _persistenceId = persistenceId;
                _fromSequenceNr = fromSequenceNr;

                _journal._session.Underlying.Wait(_journal._config.BlockingTimeout);
                _session = _journal._session.Underlying.Result;
                _journal._preparedSelectMessages.Value.Wait(_journal._config.BlockingTimeout);
                _preparedSelectMessages = _journal._preparedSelectMessages.Value.Result;
                _journal._preparedCheckInUse.Value.Wait(_journal._config.BlockingTimeout);
                _preparedCheckInUse = _journal._preparedCheckInUse.Value.Result;
                _rowEnumerator = NewEnumerator();
            }

            public bool MoveNext()
            {
                while (true)
                {
                    if (_rowEnumerator.MoveNext())
                    {
                        // entry available in current result set
                        _currentSequenceNr = _rowEnumerator.Current.GetValue<long>("sequence_nr");
                        return true;
                    }
                    if (!IsInUse())
                    {
                        // partition has never been in use, so stop
                        return false;
                    }
                    // all entries consumed, try next partition
                    _currentPartitionNr += 1;
                    _startSequenceNr = _currentSequenceNr;
                    _rowEnumerator = NewEnumerator();
                }
            }

            public void Reset()
            {
                _currentPartitionNr = _journal.PartitionNr(_fromSequenceNr);
                _currentSequenceNr = _fromSequenceNr;
                _startSequenceNr = _fromSequenceNr;
                _rowEnumerator = NewEnumerator();
            }

            public Row Current => _rowEnumerator.Current;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                _journal = null;
            }

            private IEnumerator<Row> NewEnumerator()
            {
                return
                    _session.Execute(_preparedSelectMessages.Bind(_persistenceId, _currentPartitionNr, _startSequenceNr,
                        long.MaxValue)).GetEnumerator();
            }

            private bool IsInUse()
            {
                var resultSet = _session.Execute(_preparedCheckInUse.Bind(_persistenceId, _currentPartitionNr));
                return !resultSet.IsExhausted() && resultSet.First().GetValue<bool>("used");
            }
        }

        public static object DeserializeEvent(Akka.Serialization.Serialization serialization, Row row)
        {
            return serialization.Deserialize(row.GetValue<byte[]>("event"), row.GetValue<int>("ser_id"),
                row.GetValue<string>("ser_manifest"));
        }
    }

    public class FixedRetryPolicy: IRetryPolicy
    {
        private readonly int _number;

        public FixedRetryPolicy(int number)
        {
            _number = number;
        }

        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses,
            bool dataRetrieved, int nbRetry)
        {
            return Retry(cl, nbRetry);
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks,
            int nbRetry)
        {
            return Retry(cl, nbRetry);
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return Retry(cl, nbRetry);
        }

        private RetryDecision Retry(ConsistencyLevel cl, int nbRetry)
        {
            return nbRetry < _number ? RetryDecision.Retry(cl) : RetryDecision.Rethrow();
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Journal;
using Akka.Serialization;
using Akka.Util.Internal;
using Cassandra;

namespace Akka.Persistence.Cassandra.Journal
{
    /// <summary>
    /// An Akka.NET journal implementation that writes events asynchronously to Cassandra.
    /// </summary>
    public class CassandraJournal : AsyncWriteJournal
    {
        private const string InvalidPartitionSizeException =
            "Partition size cannot change after initial table creation. (Value at creation: {0}, Currently configured value in Akka configuration: {1})";

        private static readonly Type PersistentRepresentationType = typeof (IPersistentRepresentation);

        private readonly CassandraExtension _cassandraExtension;
        private readonly Serializer _serializer;
        private readonly int _maxDeletionBatchSize;

        private ISession _session;
        private PreparedStatement _writeMessage;
        private PreparedStatement _selectHighestSequenceNumber;
        private PreparedStatement _selectMessages;
        private PreparedStatement _deleteMessagePermanent;
        private readonly LoggingRetryPolicy _deleteRetryPolicy;
        private readonly LoggingRetryPolicy _writeRetryPolicy;
        private PreparedStatement _checkInUse;
        private PreparedStatement _writeInUse;
        private PreparedStatement _selectDeletedTo;
        private PreparedStatement _insertDeletedTo;
        private int _maxTagsPerEvent;
        private IReadOnlyDictionary<string, int> _tags;

        public CassandraJournal()
        {
            _cassandraExtension = CassandraPersistence.Instance.Apply(Context.System);
            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);

            // Use setting from the persistence extension when batch deleting
            var journalSettings = _cassandraExtension.JournalSettings;
            _deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(journalSettings.DeleteRetries));
            _writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(journalSettings.WriteRetries));
            _maxDeletionBatchSize = journalSettings.MaxMessageBatchSize;
            _maxTagsPerEvent = journalSettings.MaxTagsPerEvent;
            _tags = journalSettings.Tags;
        }
        
        protected override void PreStart()
        {
            base.PreStart();

            // Create session
            CassandraJournalSettings settings = _cassandraExtension.JournalSettings;
            _session = _cassandraExtension.SessionManager.ResolveSession(settings.SessionKey);
            
            // Create keyspace if necessary and always try to create table
            if (settings.KeyspaceAutocreate)
                _session.Execute(string.Format(JournalStatements.CreateKeyspace, settings.Keyspace, settings.KeyspaceCreationOptions));

            var fullyQualifiedTableName = string.Format("{0}.{1}", settings.Keyspace, settings.Table);
            var metaTableName = $"{settings.Keyspace}.{settings.MetadataTable}";

            var createTable = string.IsNullOrWhiteSpace(settings.TableCreationProperties)
                                     ? string.Format(JournalStatements.CreateTable, fullyQualifiedTableName, string.Empty, string.Empty)
                                     : string.Format(JournalStatements.CreateTable, fullyQualifiedTableName, " WITH ",
                                                     settings.TableCreationProperties);
            var createConfigTable = string.Format(JournalStatements.CreateConfigTable, $"{settings.Keyspace}.{settings.MetadataTable}");
            var createMetaTable = string.Format(JournalStatements.CreateMetatdataTable, metaTableName);
            _session.Execute(createTable);
            _session.Execute(createConfigTable);
            _session.Execute(createMetaTable);

            // Prepare some statements against C*
            _writeMessage = _session.PrepareFormat(JournalStatements.WriteMessage, fullyQualifiedTableName);
            _deleteMessagePermanent = _session.PrepareFormat(JournalStatements.DeleteMessage, fullyQualifiedTableName);
            _selectMessages = _session.PrepareFormat(JournalStatements.SelectMessages, fullyQualifiedTableName);
            _checkInUse = _session.PrepareFormat(JournalStatements.SelectInUse, fullyQualifiedTableName);
            _writeInUse = _session.PrepareFormat(JournalStatements.WriteInUse, fullyQualifiedTableName);
            _selectDeletedTo = _session.PrepareFormat(JournalStatements.SelectDeletedTo, metaTableName);
            _insertDeletedTo = _session.PrepareFormat(JournalStatements.InsertDeletedTo, metaTableName);
            _selectHighestSequenceNumber = _session.PrepareFormat(JournalStatements.SelectHighestSequenceNumber, fullyQualifiedTableName);

            // The partition size can only be set once (the first time the table is created) so see if it's already been set
            long partitionSize = GetConfigurationValueOrDefault("partition-size", -1L);
            if (partitionSize == -1L)
            {
                // Persist the partition size specified in the cluster settings
                WriteConfigurationValue("partition-size", settings.TargetPartitionSize);
            }
            else if (partitionSize != settings.TargetPartitionSize)
            {
                throw new ConfigurationException(string.Format(InvalidPartitionSizeException, partitionSize, settings.TargetPartitionSize));
            }
        }

        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max,
                                                       Action<IPersistentRepresentation> replayCallback)
        {
            long partitionNumber = GetPartitionNumber(fromSequenceNr);

            // A sequence number may have been moved to the next partition if it was part of a batch that was too large
            // to write to a single partition
            long maxPartitionNumber = GetPartitionNumber(toSequenceNr) + 1L;
            long count = 0L;

            while (partitionNumber <= maxPartitionNumber && count < max)
            {
                // Check for header and deleted to sequence number in parallel
                RowSet[] rowSets = await GetHeaderAndDeletedTo(persistenceId, partitionNumber).ConfigureAwait(false);

                // If header doesn't exist, just bail on the non-existent partition
                if (rowSets[0].SingleOrDefault() == null)
                    return;

                // See what's been deleted in the partition and if no record found, just use long's min value
                Row deletedToRow = rowSets[1].SingleOrDefault();
                long deletedTo = deletedToRow == null ? long.MinValue : deletedToRow.GetValue<long>("sequence_number");

                // Page through messages in the partition
                bool hasNextPage = true;
                byte[] pageState = null;
                while (count < max && hasNextPage)
                {
                    // Get next page from current partition
                    IStatement getRows = _selectMessages.Bind(persistenceId, partitionNumber, fromSequenceNr, toSequenceNr)
                                                        .SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency)
                                                        .SetPageSize(_cassandraExtension.JournalSettings.MaxResultSize)
                                                        .SetPagingState(pageState)
                                                        .SetAutoPage(false);

                    RowSet messageRows = await _session.ExecuteAsync(getRows).ConfigureAwait(false);
                    pageState = messageRows.PagingState;
                    hasNextPage = pageState != null;
                    IEnumerator<IPersistentRepresentation> messagesEnumerator =
                        messageRows.Select(row => MapRowToPersistentRepresentation(row, deletedTo))
                                   .GetEnumerator();

                    // Process page
                    while (count < max && messagesEnumerator.MoveNext())
                    {
                        replayCallback(messagesEnumerator.Current);
                        count++;
                    }
                }
                
                // Go to next partition
                partitionNumber++;
            }
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            fromSequenceNr = Math.Max(1L, fromSequenceNr);

            var res = await _session.ExecuteAsync(_selectDeletedTo.Bind(persistenceId)).ConfigureAwait(false);
            var deletedToRow = res.SingleOrDefault();
            // TODO: continue to port logic to read higest sequence number
            var deletedTo = 0L;
            if (deletedToRow != null) 
                deletedTo = deletedToRow.GetValue<long>("deleted_to");

            long partitionNumber = GetPartitionNumber(fromSequenceNr);
            long currentSnr = fromSequenceNr;




            while (true)
            {
                var rowSet = await _session.ExecuteAsync(_selectHighestSequenceNumber.Bind(persistenceId, partitionNumber)).ConfigureAwait(false);

                // If header doesn't exist, just bail on the non-existent partition
                var sequenceNumberRow = rowSet.SingleOrDefault();
                if (sequenceNumberRow == null)
                    break;

                var used = sequenceNumberRow.GetValue<bool>("used");
                var nextHighestSqnr = sequenceNumberRow.GetValue<long>("sequence_number");
                if (!used) return currentSnr;
                if (nextHighestSqnr>0)
                    currentSnr = nextHighestSqnr;

                // Go to next partition
                partitionNumber++;
            }

            return currentSnr;
        }


        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {

            // It's implied by the API/docs that a batch of messages will be for a single persistence id
            var messageList = messages.ToList();

            if (!messageList.Any())
                return null;
            var maxPnr = GetPartitionNumber(((IImmutableList<IPersistentRepresentation>) messageList.Last().Payload).Last().SequenceNr);
            var firstSeq = ((IImmutableList<IPersistentRepresentation>) messageList.First().Payload).First().SequenceNr;
            var minPnr = GetPartitionNumber(firstSeq);
            var persistenceId = messageList.First().PersistenceId;
            // reading assumes sequence numbers are in the right partition or partition + 1
            // even if we did allow this it would perform terribly as large C* batches are not good
            if (maxPnr-minPnr<=1) throw new NotSupportedException("Do not support AtomicWrites that span 3 partitions. Keep AtomicWrites <= max partition size.");
            var writeTasks = messageList.Select(async write =>
            {
                var serialized = Serialize(write).Payload;

                var persistentMessages = serialized.ToList();
                

                // No need for a batch if writing a single message
                var timeUuid = TimeUuid.NewId();
                var timeBucket = new TimeBucket(timeUuid);
                if (persistentMessages.Count == 1)
                {
                    var message = persistentMessages[0];
                    var statement = BindMessageStatement(persistenceId, maxPnr, message, timeUuid, timeBucket)
                        .SetConsistencyLevel(_cassandraExtension.JournalSettings.WriteConsistency).SetRetryPolicy(_writeRetryPolicy);
                    // TODO: handle event tags here
                    return await _session.ExecuteAsync(statement);
                }

                // Use a batch and add statements for each message
                var batch = new BatchStatement();
                var tagCounts = new int[_maxTagsPerEvent];
                foreach (var message in persistentMessages)
                {
                    var boundStatement = BindMessageStatement(persistenceId, maxPnr, message, timeUuid, timeBucket);
                    if (message.Tags != null && message.Tags.Count> 0)
                    {
                        message.Tags.ForEach(tag =>
                        {
                            int tagId;
                            if (!_tags.TryGetValue(tag, out tagId)) tagId = 1;


                            tagCounts[tagId - 1] = tagCounts[tagId - 1] + 1;
                            var i = 0;
                            while (i < tagCounts.Length)
                            {
                                if (tagCounts[i] > 1)
//                                    warning("Duplicate tag identifer [{}] among tags [{}] for event from [{}]. " +
//                                                "Define known tags in cassandra-journal.tags configuration when using more than " +
//                                                "one tag per event.", (i + 1), m.tags.mkString(","), persistenceId);
                                i += 1;
                            }

                        });
                    }
                    batch.Add(boundStatement);
                }

                // in case we skip an entire partition we want to make sure the empty partition has in in-use flag so scans
                // keep going when they encounter it
                if (IsNewPartition(firstSeq) && minPnr != maxPnr) batch.Add(_writeInUse.Bind(persistenceId, minPnr));

                batch.SetConsistencyLevel(_cassandraExtension.JournalSettings.WriteConsistency).SetRetryPolicy(_writeRetryPolicy);
                return await _session.ExecuteAsync(batch);
            });



            return await Task<ImmutableList<Exception>>.Factory.ContinueWhenAll(writeTasks.ToArray(), tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());

        }

        private BoundStatement BindMessageStatement(string persistenceId, long maxPnr, Serialized message, TimeUuid timeUuid, TimeBucket timeBucket, params string[] tags)
        {
            return _writeMessage.Bind(
                persistenceId,
                maxPnr, 
                message.SequenceNr,
                timeUuid,
                timeBucket.Key,
                message.WriterUuid,
                message.SerId,
                message.SerManifest,
                message.EventManifest,
                message.SerializedData,
                null);
        }


        private SerializedAtomicWrite Serialize(AtomicWrite aw)
        {
            var result = new SerializedAtomicWrite
            {
                PersistenceId = aw.PersistenceId,
                Payload = ((IEnumerable<IPersistentRepresentation>)aw.Payload).Select(p =>
                {
                    if (!(p.Payload is Tagged)) return SerializeEvent(p, ImmutableHashSet<string>.Empty);
                    var tagged = (Tagged)p.Payload;
                    var taggedPayload = p.WithPayload(tagged);
                    return SerializeEvent(taggedPayload, tagged.Tags);
                })
            };
            return result;
        }

        private Serialized SerializeEvent(IPersistentRepresentation p, IImmutableSet<string> tags)
        {
            // Akka.Serialization.Serialization.
            var @event = p.Payload;
            var serializer = Context.System.Serialization.FindSerializerFor(@event);
            var serManifest="";
            if (serializer is SerializerWithStringManifest)
            {
                serManifest = ((SerializerWithStringManifest) serializer).Manifest(@event);
            }else if (serializer.IncludeManifest)
            {
                serManifest = @event.GetType().Name;
            }
            var serEvent = serializer.ToBinary(@event);

            // TODO: figure if we need to check for the transportInformation here
            return new Serialized
            {
                PersistenceId = p.PersistenceId,
                SequenceNr = p.SequenceNr,
                SerializedData = serEvent,
                Tags = tags,
                EventManifest = p.Manifest,
                SerManifest = serManifest,
                SerId = serializer.Identifier,
                WriterUuid = p.WriterGuid
            };
        }

        private Exception TryUnwrapException(Exception e)
        {
            var aggregateException = e as AggregateException;
            if (aggregateException != null)
            {
                aggregateException = aggregateException.Flatten();
                if (aggregateException.InnerExceptions.Count == 1)
                    return aggregateException.InnerExceptions[0];
            }
            return e;
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            long maxPartitionNumber = GetPartitionNumber(toSequenceNr) + 1L;
            long partitionNumber = 0L;

            while (partitionNumber <= maxPartitionNumber)
            {
                // Check for header and deleted to sequence number in parallel
                RowSet[] rowSets = await GetHeaderAndDeletedTo(persistenceId, partitionNumber).ConfigureAwait(false);

                // If header doesn't exist, just bail on the non-existent partition
                Row headerRow = rowSets[0].SingleOrDefault();
                if (headerRow == null)
                    return;

                // Start deleting either from the first sequence number after the last deletion, or the beginning of the partition
                Row deletedToRow = rowSets[1].SingleOrDefault();
                long deleteFrom = deletedToRow == null
                                      ? headerRow.GetValue<long>("sequence_number")
                                      : deletedToRow.GetValue<long>("sequence_number") + 1L;
                
                // Nothing to delete if we're going to start higher than the specified sequence number
                if (deleteFrom > toSequenceNr)
                    return;

                // Get the last sequence number in the partition and try to avoid tombstones by skipping deletes
                IStatement getLastMessageSequence = _selectLastMessageSequence.Bind(persistenceId, partitionNumber, deleteFrom)
                                                                              .SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency);
                RowSet lastSequenceRows = await _session.ExecuteAsync(getLastMessageSequence).ConfigureAwait(false);
                
                // If we have a sequence number, we've got messages to delete still in the partition
                Row lastSequenceRow = lastSequenceRows.SingleOrDefault();
                if (lastSequenceRow != null)
                {
                    // Delete either to the end of the partition or to the number specified, whichever comes first
                    long deleteTo = Math.Min(lastSequenceRow.GetValue<long>("sequence_number"), toSequenceNr);
                    // Permanently delete using batches in parallel
                    long batchFrom = deleteFrom;
                    long batchTo;
                    var deleteTasks = new List<Task>();
                    do
                    {
                        batchTo = Math.Min(batchFrom + _maxDeletionBatchSize - 1L, deleteTo);
                        for (long seq = batchFrom; seq <= batchTo; seq++)
                        {
                            var deleteStatement = _deleteMessagePermanent.Bind(persistenceId, partitionNumber, seq);
                            deleteStatement.SetConsistencyLevel( _cassandraExtension.JournalSettings.WriteConsistency);
                            deleteStatement.SetRetryPolicy(_deleteRetryPolicy);
                            deleteTasks.Add(_session.ExecuteAsync(deleteStatement));
                        }
                        batchFrom = batchTo + 1L;
                    } while (batchTo < deleteTo);

                    await Task.WhenAll(deleteTasks).ConfigureAwait(false);
                    
                    // If we've deleted everything we're supposed to, no need to continue
                    if (deleteTo == toSequenceNr)
                        return;
                }
                
                // Go to next partition
                partitionNumber++;
            }
        }

//        private Task<RowSet[]> GetHeaderAndDeletedTo(string persistenceId, long partitionNumber)
//        {
//            return Task.WhenAll(new[]
//            {
//                _selectHeaderSequence.Bind(persistenceId, partitionNumber).SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency),
//                _selectDeletedToSequence.Bind(persistenceId, partitionNumber).SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency)
//            }.Select(_session.ExecuteAsync));
//        }

        private IPersistentRepresentation MapRowToPersistentRepresentation(Row row, long deletedTo)
        {
            IPersistentRepresentation pr = Deserialize(row.GetValue<byte[]>("event"));
            if (pr.SequenceNr <= deletedTo)
                pr = pr.Update(pr.SequenceNr, pr.PersistenceId, true, pr.Sender, pr.WriterGuid);

            return pr;
        }

        private long GetPartitionNumber(long sequenceNumber)
        {
            return (sequenceNumber - 1L)/_cassandraExtension.JournalSettings.TargetPartitionSize;
        }

        private bool IsNewPartition(long sequenceNumber)
        {
            return (sequenceNumber - 1L)%_cassandraExtension.JournalSettings.TargetPartitionSize == 0L;
        }

        private T GetConfigurationValueOrDefault<T>(string key, T defaultValue)
        {
            IStatement bound = _selectConfigurationValue.Bind(key).SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency);
            RowSet rows = _session.Execute(bound);
            Row row = rows.SingleOrDefault();
            if (row == null)
                return defaultValue;

            IPersistentRepresentation persistent = Deserialize(row.GetValue<byte[]>("event"));
            return (T) persistent.Payload;
        }

        private void WriteConfigurationValue<T>(string key, T value)
        {
            var persistent = new Persistent(value);
            IStatement bound = _writeConfigurationValue.Bind(key, Serialize(persistent))
                                                       .SetConsistencyLevel(_cassandraExtension.JournalSettings.WriteConsistency);
            _session.Execute(bound);
        }

        private IPersistentRepresentation Deserialize(byte[] bytes)
        {
            return (IPersistentRepresentation) _serializer.FromBinary(bytes, PersistentRepresentationType);
        }

        private byte[] Serialize(IPersistentRepresentation message)
        {
            return _serializer.ToBinary(message);
        }
        
        protected override void PostStop()
        {
            base.PostStop();

            if (_cassandraExtension != null && _session != null)
            {
                _cassandraExtension.SessionManager.ReleaseSession(_session);
                _session = null;
            }
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
            if (nbRetry < _number) return RetryDecision.Retry(cl);
            else return RetryDecision.Rethrow();
        }

    }
}

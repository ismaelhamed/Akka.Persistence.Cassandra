using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Journal;
using Akka.Serialization;
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
        private PreparedStatement _writeHeader;
        private PreparedStatement _selectHeaderSequence;
        private PreparedStatement _selectLastMessageSequence;
        private PreparedStatement _selectMessages;
        private PreparedStatement _deleteMessagePermanent;
        private PreparedStatement _selectDeletedToSequence;
        private PreparedStatement _selectConfigurationValue;
        private PreparedStatement _writeConfigurationValue;
        private readonly LoggingRetryPolicy _deleteRetryPolicy;
        private readonly LoggingRetryPolicy _writeRetryPolicy;

        public CassandraJournal()
        {
            _cassandraExtension = CassandraPersistence.Instance.Apply(Context.System);
            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);

            // Use setting from the persistence extension when batch deleting
            var journalSettings = _cassandraExtension.JournalSettings;
            _deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(journalSettings.DeleteRetries));
            _writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(journalSettings.WriteRetries));
            _maxDeletionBatchSize = journalSettings.MaxMessageBatchSize;
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

            string createTable = string.IsNullOrWhiteSpace(settings.TableCreationProperties)
                                     ? string.Format(JournalStatements.CreateTable, fullyQualifiedTableName, string.Empty, string.Empty)
                                     : string.Format(JournalStatements.CreateTable, fullyQualifiedTableName, " WITH ",
                                                     settings.TableCreationProperties);
            _session.Execute(createTable);

            // Prepare some statements against C*
            _writeMessage = _session.PrepareFormat(JournalStatements.WriteMessage, fullyQualifiedTableName);
            _writeHeader = _session.PrepareFormat(JournalStatements.WriteHeader, fullyQualifiedTableName);
            _selectHeaderSequence = _session.PrepareFormat(JournalStatements.SelectHeaderSequence, fullyQualifiedTableName);
            _selectLastMessageSequence = _session.PrepareFormat(JournalStatements.SelectLastMessageSequence, fullyQualifiedTableName);
            _selectMessages = _session.PrepareFormat(JournalStatements.SelectMessages, fullyQualifiedTableName);
            _deleteMessagePermanent = _session.PrepareFormat(JournalStatements.DeleteMessagePermanent, fullyQualifiedTableName);
            _selectDeletedToSequence = _session.PrepareFormat(JournalStatements.SelectDeletedToSequence, fullyQualifiedTableName);
            _selectConfigurationValue = _session.PrepareFormat(JournalStatements.SelectConfigurationValue, fullyQualifiedTableName);
            _writeConfigurationValue = _session.PrepareFormat(JournalStatements.WriteConfigurationValue, fullyQualifiedTableName);

            // The partition size can only be set once (the first time the table is created) so see if it's already been set
            long partitionSize = GetConfigurationValueOrDefault("partition-size", -1L);
            if (partitionSize == -1L)
            {
                // Persist the partition size specified in the cluster settings
                WriteConfigurationValue("partition-size", settings.PartitionSize);
            }
            else if (partitionSize != settings.PartitionSize)
            {
                throw new ConfigurationException(string.Format(InvalidPartitionSizeException, partitionSize, settings.PartitionSize));
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
            long partitionNumber = GetPartitionNumber(fromSequenceNr);
            long maxSequenceNumber = 0L;
            while (true)
            {
                // Check for header and deleted to sequence number in parallel
                var rowSets = await GetHeaderAndDeletedTo(persistenceId, partitionNumber).ConfigureAwait(false);

                // If header doesn't exist, just bail on the non-existent partition
                if (rowSets[0].SingleOrDefault() == null)
                    break;

                // See what's been deleted in the partition and if no record found, just use long's min value
                Row deletedToRow = rowSets[1].SingleOrDefault();
                long deletedTo = deletedToRow == null ? long.MinValue : deletedToRow.GetValue<long>("sequence_number");

                // Try to avoid reading possible tombstones by skipping deleted records if higher than the fromSequenceNr provided
                long from = Math.Max(fromSequenceNr, deletedTo);

                // Get the last sequence number in the partition, skipping deleted messages
                IStatement getLastMessageSequence = _selectLastMessageSequence.Bind(persistenceId, partitionNumber, from)
                                                                              .SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency);
                RowSet sequenceRows = await _session.ExecuteAsync(getLastMessageSequence).ConfigureAwait(false);

                // If there aren't any non-deleted messages, use the delete marker's value as the max, otherwise, use whatever value was returned
                Row sequenceRow = sequenceRows.SingleOrDefault();
                maxSequenceNumber = sequenceRow == null ? Math.Max(maxSequenceNumber, deletedTo) : sequenceRow.GetValue<long>("sequence_number");

                // Go to next partition
                partitionNumber++;
            }

            return maxSequenceNumber;
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {

            // It's implied by the API/docs that a batch of messages will be for a single persistence id
            var messageList = messages.ToList();

            if (!messageList.Any())
                return null;

            var writeTasks = messageList.Select(async write =>
            {
                var persistenceId = write.PersistenceId;

                var seqNr = write.LowestSequenceNr;
                var writeHeader = IsNewPartition(seqNr);
                var partitionNumber = GetPartitionNumber(seqNr);
                var persistentMessages = ((IImmutableList<IPersistentRepresentation>)write.Payload).ToList();
                if (persistentMessages.Count > 1)
                {
                    // See if this collection of writes would span multiple partitions and if so, move all the writes to the next partition
                    var lastMessagePartition = GetPartitionNumber(write.HighestSequenceNr);
                    if (lastMessagePartition != partitionNumber)
                    {
                        partitionNumber = lastMessagePartition;
                        writeHeader = true;
                    }
                }
                // No need for a batch if writing a single message
                var timeUuid = TimeUuid.NewId();
                var timeBucket = new TimeBucket(timeUuid);
                if (persistentMessages.Count == 1 && writeHeader == false)
                {
                    var message = persistentMessages[0];
                    var statement = _writeMessage.Bind(
                        persistenceId, 
                        partitionNumber, 
                        message.SequenceNr,
                        timeUuid,
                        timeBucket.Key,
                        message.WriterGuid,
                        null, // TODO: message.SerializerId,
                        null, // TODO: message.SerializerManifest,
                        message.Manifest,
                        Serialize(message),
                        null)
                        .SetConsistencyLevel(_cassandraExtension.JournalSettings.WriteConsistency).SetRetryPolicy(_writeRetryPolicy);
                    // TODO: handle event tags here
                    return await _session.ExecuteAsync(statement);
                }

                // Use a batch and add statements for each message
                var batch = new BatchStatement();
                foreach (var message in persistentMessages)
                {
                    
                    batch.Add(_writeMessage.Bind(
                        message.PersistenceId, 
                        partitionNumber, 
                        message.SequenceNr,
                        timeUuid,
                        timeBucket.Key,
                        message.WriterGuid,
                        null, // TODO: message.SerializerId,
                        null, // TODO: message.SerializerManifest,
                        message.Manifest,
                        Serialize(message),
                        null));
                }

                // Add header if necessary
                if (writeHeader)
                    batch.Add(_writeHeader.Bind(persistenceId, partitionNumber, seqNr));

                batch.SetConsistencyLevel(_cassandraExtension.JournalSettings.WriteConsistency).SetRetryPolicy(_writeRetryPolicy);
                return await _session.ExecuteAsync(batch);
            });



            return await Task<ImmutableList<Exception>>.Factory.ContinueWhenAll(writeTasks.ToArray(), tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());

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

        private Task<RowSet[]> GetHeaderAndDeletedTo(string persistenceId, long partitionNumber)
        {
            return Task.WhenAll(new[]
            {
                _selectHeaderSequence.Bind(persistenceId, partitionNumber).SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency),
                _selectDeletedToSequence.Bind(persistenceId, partitionNumber).SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency)
            }.Select(_session.ExecuteAsync));
        }

        private IPersistentRepresentation MapRowToPersistentRepresentation(Row row, long deletedTo)
        {
            IPersistentRepresentation pr = Deserialize(row.GetValue<byte[]>("message"));
            if (pr.SequenceNr <= deletedTo)
                pr = pr.Update(pr.SequenceNr, pr.PersistenceId, true, pr.Sender, pr.WriterGuid);

            return pr;
        }

        private long GetPartitionNumber(long sequenceNumber)
        {
            return (sequenceNumber - 1L)/_cassandraExtension.JournalSettings.PartitionSize;
        }

        private bool IsNewPartition(long sequenceNumber)
        {
            return (sequenceNumber - 1L)%_cassandraExtension.JournalSettings.PartitionSize == 0L;
        }

        private T GetConfigurationValueOrDefault<T>(string key, T defaultValue)
        {
            IStatement bound = _selectConfigurationValue.Bind(key).SetConsistencyLevel(_cassandraExtension.JournalSettings.ReadConsistency);
            RowSet rows = _session.Execute(bound);
            Row row = rows.SingleOrDefault();
            if (row == null)
                return defaultValue;

            IPersistentRepresentation persistent = Deserialize(row.GetValue<byte[]>("message"));
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

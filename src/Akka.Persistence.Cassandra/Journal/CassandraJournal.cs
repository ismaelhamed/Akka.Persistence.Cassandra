using System;
using System.Collections;
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

        private readonly CassandraExtension _cassandraExtension;

        private ISession _session;
        private PreparedStatement _writeMessage;
        private PreparedStatement _selectHighestSequenceNumber;
        private PreparedStatement _selectMessages;
        private PreparedStatement _deleteMessages;
        private PreparedStatement _checkInUse;
        private PreparedStatement _writeInUse;
        private PreparedStatement _selectDeletedTo;
        private PreparedStatement _insertDeletedTo;
        private readonly int _maxTagsPerEvent;
        private readonly IReadOnlyDictionary<string, int> _tags;
        private readonly LoggingRetryPolicy _deleteRetryPolicy;
        private readonly LoggingRetryPolicy _writeRetryPolicy;
        private long _targetPartitionSize;
        private int _maxResultSizeReplay;

        public CassandraJournal()
        {
            _cassandraExtension = CassandraPersistence.Instance.Apply(Context.System);

            // Use setting from the persistence extension when batch deleting
            var journalSettings = _cassandraExtension.JournalSettings;
            _deleteRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(journalSettings.DeleteRetries));
            _writeRetryPolicy = new LoggingRetryPolicy(new FixedRetryPolicy(journalSettings.WriteRetries));
            _maxTagsPerEvent = journalSettings.MaxTagsPerEvent;
            _tags = journalSettings.Tags;
        }
        
        protected override void PreStart()
        {
            base.PreStart();

            // Create session
            CassandraJournalSettings settings = _cassandraExtension.JournalSettings;
            _session = _cassandraExtension.SessionManager.ResolveSession(settings.SessionKey);
            
            // Create keyspace if necessary and always try to create the tables
            if (settings.KeyspaceAutocreate)
                _session.Execute(string.Format(JournalStatements.CreateKeyspace, settings.Keyspace, settings.KeyspaceCreationOptions));

            var fullyQualifiedTableName = $"{settings.Keyspace}.{settings.Table}";
            var metaTableName = $"{settings.Keyspace}.{settings.MetadataTable}";
            var configTable = settings.ConfigTable;
            var createTable = string.IsNullOrWhiteSpace(settings.TableCreationProperties)
                                     ? string.Format(JournalStatements.CreateTable, fullyQualifiedTableName, string.Empty, string.Empty)
                                     : string.Format(JournalStatements.CreateTable, fullyQualifiedTableName, " WITH ",
                                                     settings.TableCreationProperties);
            var createConfigTable = string.Format(JournalStatements.CreateConfigTable, $"{settings.Keyspace}.{configTable}");
            var createMetaTable = string.Format(JournalStatements.CreateMetatdataTable, metaTableName);
            _session.Execute(createTable);
            _session.Execute(createConfigTable);
            _session.Execute(createMetaTable);

            // Prepare some statements against C*
            _writeMessage = _session.PrepareFormat(JournalStatements.WriteMessage, fullyQualifiedTableName);
            _deleteMessages = _session.PrepareFormat(JournalStatements.DeleteMessages, fullyQualifiedTableName);
            _selectMessages = _session.PrepareFormat(JournalStatements.SelectMessages, fullyQualifiedTableName);
            _checkInUse = _session.PrepareFormat(JournalStatements.SelectInUse, fullyQualifiedTableName);
            _writeInUse = _session.PrepareFormat(JournalStatements.WriteInUse, fullyQualifiedTableName);
            _selectDeletedTo = _session.PrepareFormat(JournalStatements.SelectDeletedTo, metaTableName);
            _insertDeletedTo = _session.PrepareFormat(JournalStatements.InsertDeletedTo, metaTableName);
            _selectHighestSequenceNumber = _session.PrepareFormat(JournalStatements.SelectHighestSequenceNumber, fullyQualifiedTableName);

            _targetPartitionSize = settings.TargetPartitionSize;
            _maxResultSizeReplay = settings.MaxResultSizeReplay;

            // The partition size can only be set once (the first time the table is created) so see if it's already been set
            CheckCorrectPartitionSize(configTable);

        }

        private void CheckCorrectPartitionSize(string configTableName)
        {
            var storedConfig = _session.Execute(_session.PrepareFormat(JournalStatements.SelectConfig, configTableName).Bind())
                .ToDictionary(r => r.GetValue<string>("property"), r => r.GetValue<string>("value"));
            string oldValue;
            Action<string> asserPartitionSize = size =>
            {
                int oldPartitionSize;
                if (!int.TryParse(size, out oldPartitionSize) || oldPartitionSize != _targetPartitionSize)
                    throw new ConfigurationException(string.Format(InvalidPartitionSizeException, size,
                        _targetPartitionSize));

            };
            if (storedConfig.TryGetValue("target-partition-size", out oldValue))
            {
                asserPartitionSize(oldValue);
            }
            else
            {
                var query = _session.Execute(
                    _session.PrepareFormat(JournalStatements.WriteConfig, configTableName)
                        .Bind("target-partition-size", _targetPartitionSize));
                var rowEnum = query.GetEnumerator();
                rowEnum.MoveNext();
                if (!rowEnum.Current.GetValue<bool>("applied"))
                {
                    while(rowEnum.MoveNext())
                    {
                        oldValue = rowEnum.Current.GetValue<string>("value");
                        asserPartitionSize(oldValue);
                    }
                    
                }
            }
        }

        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max,
                                                       Action<IPersistentRepresentation> replayCallback)
        {
            var readJournal = new CassandraReadJournal(context.System as ExtendedActorSystem,
                context.System.Settings.Config.GetConfig("cassandra-query-journal"));
            
            var events = readJournal.EventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, max, _maxResultSizeReplay, null, "asyncReplayMessages");
            return events.RunForeach(replayCallback, context.System.Materializer());
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            fromSequenceNr = Math.Max(1L, fromSequenceNr);

            var highestDeletedSqnr = await HighestDeletedSequenceNumber(persistenceId);
            fromSequenceNr = Math.Max(fromSequenceNr, highestDeletedSqnr);
            var partitionNumber = GetPartitionNumber(fromSequenceNr);
            var currentSnr = fromSequenceNr;




            while (true)
            {
                var rowSet = await _session.ExecuteAsync(_selectHighestSequenceNumber.Bind(persistenceId, partitionNumber)).ConfigureAwait(false);

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

        private async Task<long> HighestDeletedSequenceNumber(string persistenceId)
        {
            var res = await _session.ExecuteAsync(_selectDeletedTo.Bind(persistenceId)).ConfigureAwait(false);
            var deletedToRow = res.SingleOrDefault();
            var deletedTo = 0L;
            if (deletedToRow != null)
                deletedTo = deletedToRow.GetValue<long>("deleted_to");
            return deletedTo;
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
                    // TODO: do publishTagNotification here
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
                            // TODO: how to set tag values for the existing boundStatement?? Scala version supports the following syntax: bs.setString("tag" + tagId, tag)
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
                tags);
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

            // TODO: figure if we need to check for the transportInformation here. Cant find currentTransportInformation so far....
            // Below is what akka does:
            // serialize actor references with full address information (defaultAddress)
            // transportInformation match {
            //    case Some(ti) ⇒ Serialization.currentTransportInformation.withValue(ti) { doSerializeEvent() }
            //    case None     ⇒ doSerializeEvent()
            // }
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
            

            var fromSnr = ReadLowestSequenceNumber(persistenceId, 1L);
            var lowestPartitionNumber = GetPartitionNumber(fromSnr);
            var toSnr = Math.Min(toSequenceNr, await ReadHighestSequenceNrAsync(persistenceId, fromSnr));
            var maxPartitionNumber = GetPartitionNumber(toSequenceNr) + 1L;
            
            for (var partitionNumber = lowestPartitionNumber; partitionNumber < maxPartitionNumber; partitionNumber++)
            {
                /* TODO: implement cassandra2x compatibility 
                var rowSet =
                    await _session.ExecuteAsync(_selectHighestSequenceNumber.Bind(persistenceId, partitionNumber));
                var row = rowSet.First();
                var partitionInfo = new PartitionInfo
                {
                    PartitionNumber = partitionNumber,
                    MinSequenceNumber = MinSequenceNumber(partitionNumber),
                    MaxSequenceNumber = Math.Min(row.GetValue<long>("sequence_number"), toSnr)
                };
                */
                await _session.ExecuteAsync(_deleteMessages.Bind(persistenceId, partitionNumber, toSnr).SetRetryPolicy(_deleteRetryPolicy));
            }
            await _session.ExecuteAsync(_insertDeletedTo.Bind(persistenceId, toSnr).SetRetryPolicy(_deleteRetryPolicy));
        }

        private long ReadLowestSequenceNumber(string persistenceId, long fromSequenceNumber)
        {
            var iterator = new MessageIterator(persistenceId, fromSequenceNumber, long.MaxValue, long.MaxValue, this);
            var result = fromSequenceNumber;
            while (iterator.MoveNext() && !iterator.Current.IsDeleted) result = iterator.Current.SequenceNr;
            return result;

        }


        private long GetPartitionNumber(long sequenceNumber)
        {
            return (sequenceNumber - 1L)/_cassandraExtension.JournalSettings.TargetPartitionSize;
        }

        private bool IsNewPartition(long sequenceNumber)
        {
            return (sequenceNumber - 1L)%_cassandraExtension.JournalSettings.TargetPartitionSize == 0L;
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


        private class SerializedAtomicWrite
        {
            public string PersistenceId { get; set; }
            public IEnumerable<Serialized> Payload { get; set; }
        }

        private class Serialized
        {
            public string PersistenceId { get; set; }
            public long SequenceNr { get; set; }
            public byte[] SerializedData { get; set; }
            public IImmutableSet<string> Tags { get; set; }
            public string EventManifest { get; set; }
            public string SerManifest { get; set; }
            public int SerId { get; set; }
            public string WriterUuid { get; set; }
        }


        private class RowIteratorParams
        {
            public ISession Session { get; }
            public PreparedStatement SelectMessagesStatement { get; }
            public PreparedStatement CheckInUseStatement { get; }

            public RowIteratorParams(ISession session, PreparedStatement selectMessagesStatement, PreparedStatement checkInUseStatement)
            {
                Session = session;
                SelectMessagesStatement = selectMessagesStatement;
                CheckInUseStatement = checkInUseStatement;
            }
        }
        private class RowIterator: IEnumerator<Row>
        {
            private readonly string _persistenceId;
            private long _fromSequenceNr;

            private long _currentSnr;
            private long _currentPnr;
            private IEnumerator<Row> _iterator;
            private readonly RowIteratorParams _iteratorParams;
            private readonly Func<IEnumerator<Row>> _newIterator;
            public RowIterator(string persistenceId, long fromSequenceNr, long toSequenceNr, CassandraJournal jounrnal)
            {
                _persistenceId = persistenceId;
                _fromSequenceNr = fromSequenceNr;
                _currentSnr = fromSequenceNr;
                _currentPnr = jounrnal.GetPartitionNumber(fromSequenceNr);
                _iteratorParams = new RowIteratorParams(jounrnal._session, jounrnal._selectMessages, jounrnal._checkInUse);
                _newIterator =
                    () =>
                        jounrnal._session.Execute(_iteratorParams.SelectMessagesStatement.Bind(
                            _persistenceId, _currentPnr, _fromSequenceNr, toSequenceNr)).GetEnumerator();
                _iterator = _newIterator();
            }
            private bool IsInUse(string persistenceId, long currentPartitionNumber)
            {
                var resultSet = _iteratorParams.Session.Execute(_iteratorParams.CheckInUseStatement.Bind(persistenceId, currentPartitionNumber));
                return !resultSet.IsExhausted() && resultSet.First().GetValue<bool>("used");
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public Row Current => _iterator.Current;

            public bool MoveNext()
            {
                if (_iterator.MoveNext())
                {
                    _currentSnr = _iterator.Current.GetValue<long>("sequence_number");
                    return true;
                }
                if (!IsInUse(_persistenceId, _currentPnr))
                {
                    return false;
                }
                _currentPnr = _currentPnr+1;
                _fromSequenceNr = _currentSnr;
                _iterator = _newIterator();
                return MoveNext();
            }

            public void Reset()
            {
                _iterator.Reset();
            }

            object IEnumerator.Current => Current;
        }

        private class MessageIterator: IEnumerator<IPersistentRepresentation>
        {
            private readonly long _max;
            private IPersistentRepresentation _current;
            private IPersistentRepresentation _next;
            private long _mcnt;
            private readonly RowIterator _iter;

            public MessageIterator(string persistenceId, long fromSequenceNr, long toSequenceNr, long max, CassandraJournal jounrnal)
            {
                _max = max;
                var initialFromSequenceNr = Math.Max(jounrnal.HighestDeletedSequenceNumber(persistenceId).Result + 1,
                    fromSequenceNr);
                _iter = new RowIterator(
                    persistenceId,
                    initialFromSequenceNr, 
                    toSequenceNr, 
                    jounrnal);
            }

            private void Fetch()
            {
                _current = _next;
                _next = null;
                while (_iter.MoveNext() && _next == null)
                {
                    var row = _iter.Current;
                    var snr = row.GetValue<long>("sequence_number");
                    var msg = row.GetValue<byte[]>("message");
                    if (msg!=null) throw new NotImplementedException("Backward compatibility is not yet implemented");
                    var result = new Persistent(
                        payload: DeserializeEvent(row),
                        sequenceNr:row.GetValue<long>("sequence_number"),
                        persistenceId:row.GetValue<string>("persistence_id"),
                        manifest:row.GetValue<string>("event_manifest"),
                        isDeleted:false,
                        sender:null,
                        writerGuid:row.GetValue<string>("writer_uuid")
                        );
                    // there may be duplicates returned by iter
                    // (on scan boundaries within a partition)
                    if (snr == _current.SequenceNr) _current = result;
                    else _next = result;
                }
            }

            private object DeserializeEvent(Row row)
            {
                return Context.System.Serialization.Deserialize(
                    row.GetValue<byte[]>("event"),
                    row.GetValue<int>("ser_id"),
                    row.GetValue<string>("ser_manifest"));
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public bool MoveNext()
            {
                Fetch();
                _mcnt = _mcnt + 1;
                return _next != null && _mcnt < _max;

            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public IPersistentRepresentation Current => _current;

            object IEnumerator.Current => Current;
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

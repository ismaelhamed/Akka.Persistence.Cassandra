using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Cassandra.Query;
using Akka.Streams;

namespace Akka.Persistence.Cassandra.Journal
{
    public partial class CassandraJournal
    {
        private readonly ActorMaterializer _materializer;
        private readonly CassandraReadJournal _queries;

        public override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr,
            long toSequenceNr, long max, Action<IPersistentRepresentation> replayCallback)
        {
            return _queries
                .EventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, max, _config.ReplayMaxResultSize,
                    null, "asyncReplayMessages", _config.ReadConsistency)
                .RunForeach(replayCallback, _materializer);
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            Task writeTask;
            if (_writeInProgress.TryGetValue(persistenceId, out writeTask))
            {
                await writeTask;
            }

            var highestDeletedSequenceNr = await HighestDeletedSequenceNrAsync(persistenceId);

            return await FindHighestSequenceNrAsync(persistenceId, Math.Max(fromSequenceNr, highestDeletedSequenceNr));
        }

        private async Task<long> FindHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var currentPartitionNr = PartitionNr(fromSequenceNr);
            var currentSequenceNr = fromSequenceNr;

            // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
            var preparedStatement = await _preparedSelectHighestSequenceNr.Value;
            while (true)
            {
                var boundSelectHighestSequenceNr = preparedStatement.Bind(persistenceId, currentPartitionNr);
                var rs = await _session.Select(boundSelectHighestSequenceNr);
                var row = rs.FirstOrDefault();
                if (row == null)
                {
                    // never been to this partition
                    break;
                }

                var used = row.GetValue<bool>("used");
                var nextHighest = row.GetValue<long?>("sequence_nr") ?? 0;
                if (!used)
                {
                    // don't currently explicitly set false
                    break;
                }
                if (nextHighest == 0)
                {
                    // everything deleted in this partition, move to the next
                    currentPartitionNr += 1;
                }
                else
                {
                    currentPartitionNr += 1;
                    currentSequenceNr = nextHighest;
                }
            }
            return currentSequenceNr;
        }

        private async Task<long> HighestDeletedSequenceNrAsync(string persistenceId)
        {
            var selectDeletedTo = await _preparedSelectDeletedTo.Value;
            var boundSelectDeletedTo = selectDeletedTo.Bind(persistenceId);
            var rowSet = await _session.Select(boundSelectDeletedTo);
            var row = rowSet.FirstOrDefault();
            return row?.GetValue<long>("deleted_to") ?? 0;
        }
    }
}
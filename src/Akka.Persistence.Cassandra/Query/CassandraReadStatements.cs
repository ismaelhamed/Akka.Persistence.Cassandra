//-----------------------------------------------------------------------
// <copyright file="CassandraReadStatements.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

namespace Akka.Persistence.Cassandra.Query
{
    internal class CassandraReadStatements
    {
        private readonly string _selectEventsByTag;

        public CassandraReadStatements(CassandraReadJournalConfig config)
        {
            string eventsByTagViewName = $"{config.Keyspace}.{config.EventsByTagView}";
            string tableName = $"{config.Keyspace}.{config.Table}";

            _selectEventsByTag = $@"
SELECT * FROM {eventsByTagViewName}{{0}} WHERE
    tag{{0}} = ? AND
    timebucket = ? AND
    timestamp > ? AND
    timestamp <= ?
    ORDER BY timestamp ASC
    LIMIT ?";
            SelectDistinctPersistenceIds = $"SELECT DISTINCT persistence_id, partition_nr FROM {tableName }";
        }

        public string SelectEventsByTag(int tagId) => string.Format(_selectEventsByTag, tagId);

        public string SelectDistinctPersistenceIds { get; }
    }
}
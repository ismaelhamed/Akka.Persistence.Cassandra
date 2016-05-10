using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Compaction;
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// Abstract class for parsing common settings used by both the Journal and Snapshot store from HOCON configuration.
    /// </summary>
    public class CassandraPluginConfig
    {
        private static readonly Regex KeyspaceAndTableNameRegex =
            new Regex("^(\"[a-zA-Z]{1}[\\w]{0,47}\"|[a-zA-Z]{1}[\\w]{0,47})$");

        public CassandraPluginConfig(ActorSystem system, Config config)
        {
            Keyspace = ValidateKeyspaceName(config.GetString("keyspace"));
            Table = ValidateTableName(config.GetString("table"));
            MetadataTable = ValidateTableName(config.GetString("metadata-table"));
            ConfigTable = ValidateTableName(config.GetString("config-table"));

            TableCompactionStrategy = CassandraCompactionStrategy.Create(config.GetConfig("table-compaction-strategy"));

            KeyspaceAutocreate = config.GetBoolean("keyspace-autocreate");
            TablesAutocreate = config.GetBoolean("tables-autocreate");

            ConnectionRetries = config.GetInt("connect-retries");
            ConnectionRetryDelay = config.GetTimeSpan("connect-retry-delay");

            ReplicationStrategy = GetReplicationStrategy(config.GetString("replication-strategy"),
                config.GetInt("replication-factor"), config.GetStringList("data-center-replication-factors"));

            ReadConsistency =
                (ConsistencyLevel) Enum.Parse(typeof(ConsistencyLevel), config.GetString("read-consistency"), true);
            WriteConsistency =
                (ConsistencyLevel) Enum.Parse(typeof(ConsistencyLevel), config.GetString("write-consistency"), true);

            BlockingDispatcherId = config.GetString("blocking-dispatcher");

            // Quote keyspace and table if necessary
            if (config.GetBoolean("use-quoted-identifiers"))
            {
                Keyspace = $"\"{Keyspace}\"";
                Table = $"\"{Keyspace}\"";
            }

            SessionProvider = GetSessionProvider(system, config);
        }

        /// <summary>
        /// The keyspace to be created/used.
        /// </summary>
        public string Keyspace { get; }

        /// <summary>
        /// Name of the table to be created/used.
        /// </summary>
        public string Table { get; }

        /// <summary>
        /// Name of the table to be created/used for storing metadata.
        /// </summary>
        public string MetadataTable { get; }

        /// <summary>
        /// Name of the table to be created/used for journal config.
        /// </summary>
        public string ConfigTable { get; }

        /// <summary>
        /// Compaction strategy for journal/snapshot tables
        /// </summary>
        public ICassandraCompactionStrategy TableCompactionStrategy { get; }

        /// <summary>
        /// When true the plugin will automatically try to create the keyspace if it doesn't already exist on start.
        /// </summary>
        public bool KeyspaceAutocreate { get; }

        /// <summary>
        /// When true the plugin will automatically try to create the tables if it doesn't already exist on start.
        /// </summary>
        public bool TablesAutocreate { get; }

        /// <summary>
        /// Number of retries before giving up connecting to the cluster
        /// </summary>
        public int ConnectionRetries { get; private set; }

        /// <summary>
        /// Delay between connection retries
        /// </summary>
        public TimeSpan ConnectionRetryDelay { get; private set; }

        /// <summary>
        // Replication strategy to use. SimpleStrategy or NetworkTopologyStrategy
        /// </summary>
        public string ReplicationStrategy { get; private set; }

        /// <summary>
        /// Consistency level for reads.
        /// </summary>
        public ConsistencyLevel ReadConsistency { get; private set; }

        /// <summary>
        /// Consistency level for writes.
        /// </summary>
        public ConsistencyLevel WriteConsistency { get; private set; }

        /// <summary>
        /// Dispatcher for potentially blocking tasks.
        /// </summary>
        public string BlockingDispatcherId { get; private set; }

        public ISessionProvider SessionProvider { get; private set; }

        // TODO FIXME temporary until we have fixed all blocking
        internal TimeSpan BlockingTimeout { get; } = TimeSpan.FromSeconds(10);

        public static string GetReplicationStrategy(string strategy, int replicationFactor,
            ICollection<string> dataCenterReplicationFactors)
        {
            switch (strategy.ToLowerInvariant())
            {
                case "simplestrategy":
                    return $"'SimpleStrategy','replication_factor':{replicationFactor}";
                case "networktopologystrategy":
                    return
                        $"'NetworkTopologyStrategy',{GetDataCenterReplicationFactorList(dataCenterReplicationFactors)}";
                default:
                    throw new ArgumentException($"{strategy} as replication strategy is unknown and not supported.",
                        nameof(strategy));
            }
        }

        private static string GetDataCenterReplicationFactorList(ICollection<string> dataCenterReplicationFactors)
        {
            if (dataCenterReplicationFactors == null || dataCenterReplicationFactors.Count == 0)
                throw new ArgumentException(
                    "data-center-replication-factors cannot be empty when using NetworkTopologyStrategy");
            var result = dataCenterReplicationFactors.Select(dataCenterReplicationFactor =>
            {
                var parts = dataCenterReplicationFactor.Split(':');
                if (parts.Length != 2)
                    throw new ArgumentException(
                        $"A data-center-replication-factor must have the form [dataCenterName:replicationFactor] but was: {dataCenterReplicationFactor}.");
                return $"'{parts[0]}':{parts[1]}";
            });
            return string.Join(",", result);
        }

        private static ISessionProvider GetSessionProvider(ActorSystem system, Config config)
        {
            var typeName = config.GetString("session-provider");
            var type = Type.GetType(typeName, true);

            try
            {
                return (ISessionProvider) Activator.CreateInstance(type, system, config);
            }
            catch
            {
                try
                {
                    return (ISessionProvider) Activator.CreateInstance(type, system);
                }
                catch
                {
                    try
                    {
                        return (ISessionProvider) Activator.CreateInstance(type);

                    }
                    catch (Exception ex)
                    {
                        throw new ArgumentException(
                            $"Unable to create ISessionProvider instance for class [{typeName}]", ex);
                    }
                }
            }
        }

        /// <summary>
        /// Validates that the supplied keyspace name is valid based Cassandra's keyspace name requirements.
        /// See http://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html.
        /// </summary>
        /// <param name="keyspaceName">the keyspace name to validate</param>
        /// <returns>table name if valid, throws ArgumentException otherwise</returns>
        internal static string ValidateKeyspaceName(string keyspaceName)
        {
            if (KeyspaceAndTableNameRegex.IsMatch(keyspaceName))
                return keyspaceName;
            throw new ArgumentException(
                $"Invalid keyspace name. A keyspace name may contain 48 or fewer alphanumeric charachters and underscores. Value was: {keyspaceName}");
        }

        /// <summary>
        /// Validates that the supplied table name meets Cassandra's table name requirements.
        /// See https://cassandra.apache.org/doc/cql3/CQL.html#createTableStmt.
        /// </summary>
        /// <param name="tableName">the table name to validate</param>
        /// <returns>table name if valid, throws ArgumentException otherwise</returns>
        internal static string ValidateTableName(string tableName)
        {
            if (KeyspaceAndTableNameRegex.IsMatch(tableName))
                return tableName;
            throw new ArgumentException(
                $"Invalid table name. A table name may contain 48 or fewer alphanumeric charachters and underscores. Value was: {tableName}");
        }
    }
}
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
    public class StorePathPasswordConfig
    {
        public StorePathPasswordConfig(string path, string password)
        {
            Path = path;
            Password = password;
        }

        public string Path { get; }
        public string Password { get; }
    }

    /// <summary>
    /// Abstract class for parsing common settings used by both the Journal and Snapshot store from HOCON configuration.
    /// </summary>
    public abstract class CassandraSettings
    {
        private static readonly Regex KeyspaceNameRegex = new Regex("^(\"[a-zA-Z]{1}[\\w]{0,47}\"|[a-zA-Z]{1}[\\w]{0,47})$");
        /// <summary>
        /// The name (key) of the session to use when resolving an ISession instance. When using default session management,
        /// this points at configuration under the "cassandra-sessions" section where the session's configuration is found.
        /// </summary>
        public string SessionKey { get; private set; }

        /// <summary>
        /// The keyspace to be created/used.
        /// </summary>
        public string Keyspace { get; }

        /// <summary>
        /// A string to be appended to the CREATE KEYSPACE statement after the WITH clause when the keyspace is 
        /// automatically created. Use this to define options like replication strategy.
        /// </summary>
        public string KeyspaceCreationOptions { get; private set; }

        /// <summary>
        /// When true the plugin will automatically try to create the keyspace if it doesn't already exist on start.
        /// </summary>
        public bool KeyspaceAutocreate { get; private set; }

        /// <summary>
        /// Name of the table to be created/used.
        /// </summary>
        public string Table { get; private set; }

        /// <summary>
        /// Name of the table to be created/used for journal config.
        /// </summary>
        public string ConfigTable { get; private set; }

        /// <summary>
        /// Name of the table to be created/used for storing metadata.
        /// </summary>
        public string MetadataTable { get; private set; }

        /// <summary>
        /// A string to be appended to the CREATE TABLE statement after the WITH clause. Use this to define things
        /// like gc_grace_seconds or one of the many other table options.
        /// </summary>
        public string TableCreationProperties { get; private set; }

        public ICassandraCompactionStrategy TableCompactionStrategy { get; private set; }

        /// <summary>
        /// When true the plugin will automatically try to create the tables if it doesn't already exist on start.
        /// </summary>
        public bool TablesAutocreate { get; private set; }

        public int ConnectionRetries { get; private set; }

        public TimeSpan ConnectionRetryDelay { get; private set; }

        public string ReplicationStrategy { get; private set; }

        /// <summary>
        /// Consistency level for reads.
        /// </summary>
        public ConsistencyLevel ReadConsistency { get; private set; }

        /// <summary>
        /// Consistency level for writes.
        /// </summary>
        public ConsistencyLevel WriteConsistency { get; private set; }

        public string BlockingDispatcherId { get; private set; }

        public ISessionProvider SessionProvider { get; private set; }

        // TODO FIXME temporary until we have fixed all blocking
        public TimeSpan BlockingTimeout { get; } = TimeSpan.FromSeconds(10);

        protected CassandraSettings(ActorSystem system, Config config)
        {
            SessionKey = config.GetString("session-key");

            Keyspace = config.GetString("keyspace");
            KeyspaceCreationOptions = config.GetString("keyspace-creation-options");
            TableCompactionStrategy = CassandraCompactionStrategy.Create(config.GetConfig("table-compaction-strategy"));
            KeyspaceAutocreate = config.GetBoolean("keyspace-autocreate");

            Table = config.GetString("table");
            TableCreationProperties = config.GetString("table-creation-properties");
            ConfigTable = config.GetString("config-table");
            MetadataTable = config.GetString("metadata-table");
            TablesAutocreate = config.GetBoolean("tables-autocreate");
            ConfigTable = ValidateTableName(config.GetString("config-table"));
            MetadataTable = config.GetString("metadata-table");

            // Quote keyspace and table if necessary
            if (config.GetBoolean("use-quoted-identifiers"))
            {
                Keyspace = $"\"{Keyspace}\"";
                Table = $"\"{Keyspace}\"";
            }

            ConnectionRetries = config.GetInt("connect-retries");
            ConnectionRetryDelay = config.GetTimeSpan("connect-retry-delay");

            ReplicationStrategy = GetReplicationStrategy(config.GetString("replication-strategy"),
                config.GetInt("replication-factor"), config.GetStringList("data-center-replication-factors"));

            ReadConsistency = (ConsistencyLevel) Enum.Parse(typeof(ConsistencyLevel), config.GetString("read-consistency"), true);
            WriteConsistency = (ConsistencyLevel) Enum.Parse(typeof(ConsistencyLevel), config.GetString("write-consistency"), true);

            BlockingDispatcherId = config.GetString("blocking-dispatcher");

            SessionProvider = GetSessionProvider(system, config);
        }

        private string GetReplicationStrategy(string strategy, int replicationFactor, ICollection<string> dataCenterReplicationFactors)
        {
            switch (strategy.ToLowerInvariant())
            {
                case "simplestrategy":
                    return $"'SimpleStrategy','replication_factor':{replicationFactor}";
                case "networktopologystrategy":
                    return
                        $"'NetworkTopologyStrategy',{GetDataCenterReplicationFactorList(dataCenterReplicationFactors)}";
                default:
                    throw new ArgumentException($"{strategy} as replication strategy is unknown and not supported.", nameof(strategy));
            }
        }

        private static string GetDataCenterReplicationFactorList(ICollection<string> dataCenterReplicationFactors)
        {
            if (dataCenterReplicationFactors == null || dataCenterReplicationFactors.Count == 0)
                throw new ArgumentException("data-center-replication-factors cannot be empty when using NetworkTopologyStrategy");
            var result = dataCenterReplicationFactors.Select(dataCenterReplicationFactor =>
            {
                var parts = dataCenterReplicationFactor.Split(':');
                if (dataCenterReplicationFactors.Count != 2)
                    throw new ArgumentException(
                        $"A data-center-replication-factor must have the form [dataCenterName:replicationFactor] but was: {dataCenterReplicationFactor}.");
                return $"'{parts[0]}':{parts[1]}";
            });
            return string.Join(",", result);
        }

        private ISessionProvider GetSessionProvider(ActorSystem system, Config config)
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
        /// Validates that the supplied table name meets Cassandra's table name requirements.
        /// According to docs here: https://cassandra.apache.org/doc/cql3/CQL.html#createTableStmt.
        /// </summary>
        /// <param name="tableName">the table name to validate</param>
        /// <returns>table name if valid, throws ArgumentException otherwise</returns>
        public string ValidateTableName(string tableName)
        {
            if (KeyspaceNameRegex.IsMatch(tableName))
                return tableName;
            throw new ArgumentException(
                $"Invalid table name. A table name may contain 48 or fewer alphanumeric charachters and underscores. Value was: {tableName}");
        }
    }
}

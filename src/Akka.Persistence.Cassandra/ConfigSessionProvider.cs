using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// <para>
    /// Default implementation of the <see cref="ISessionProvider"/> that is used for creating the
    /// Cassandra Session. This class is building the Cluster from configuration
    /// properties.
    /// </para>
    /// <para>
    /// You may create a subclass of this that performs lookup the contact points
    /// of the Cassandra cluster asynchronously instead of reading them in the
    /// configuration. Such a subclass should override the <see cref="LookupContactPoints"/>
    /// method.
    /// </para>
    /// <para>
    /// The implementation is defined in configuration `session-provider` property.
    /// The config parameter is the config section of the plugin.
    /// </para>
    /// </summary>
    public class ConfigSessionProvider : ISessionProvider
    {
        private readonly Config _config;

        public ConfigSessionProvider(ActorSystem system, Config config)
        {
            System = system;
            _config = config;
            FetchSize = config.GetInt("max-result-size");
            var protocolVersion = config.GetString("protocol-version");
            ProtocolVersion = string.IsNullOrEmpty(protocolVersion) ? null : (byte?)config.GetInt("protocol-version");
            var connectionPoolConfig = config.GetConfig("connection-pool");
            PoolingOptions = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local,
                    connectionPoolConfig.GetInt("core-connections-per-host-local"))
                .SetMaxConnectionsPerHost(HostDistance.Local,
                    connectionPoolConfig.GetInt("max-connections-per-host-local"))
                .SetCoreConnectionsPerHost(HostDistance.Remote,
                    connectionPoolConfig.GetInt("core-connections-per-host-remote"))
                .SetMaxConnectionsPerHost(HostDistance.Remote,
                    connectionPoolConfig.GetInt("max-connections-per-host-remote"))
                .SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Local,
                    connectionPoolConfig.GetInt("min-simultaneous-requests-per-connection-threshold-local"))
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local,
                    connectionPoolConfig.GetInt("max-simultaneous-requests-per-connection-threshold-local"))
                .SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Remote,
                    connectionPoolConfig.GetInt("min-simultaneous-requests-per-connection-threshold-remote"))
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Remote,
                    connectionPoolConfig.GetInt("max-simultaneous-requests-per-connection-threshold-remote"))
                .SetHeartBeatInterval(connectionPoolConfig.GetInt("heart-beat-interval"));
        }

        protected ActorSystem System { get; }

        public int FetchSize { get; }

        public byte? ProtocolVersion { get; }

        public PoolingOptions PoolingOptions { get; }

        public async Task<ISession> Connect()
        {
            var clusterId = _config.GetString("cluster-id");

            var builder = await ClusterBuilder(clusterId);
            return builder.Build().Connect();
        }

        private async Task<Builder> ClusterBuilder(string clusterId)
        {
            var contactPoints = await LookupContactPoints(clusterId);

            var builder = new Builder()
                .AddContactPoints(contactPoints)
                .WithPoolingOptions(PoolingOptions)
                .WithQueryOptions(new QueryOptions().SetPageSize(FetchSize));
            if (ProtocolVersion.HasValue)
                builder.WithMaxProtocolVersion(ProtocolVersion.Value);

            var username = _config.GetString("authentication.username");
            if (!string.IsNullOrEmpty(username))
                builder.WithCredentials(username, _config.GetString("authentication.password"));

            var localDatacenter = _config.GetString("local-datacenter");
            if (!string.IsNullOrEmpty(localDatacenter))
                builder.WithLoadBalancingPolicy(
                    new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDatacenter)));

            // TODO Aditional ssl settings
            if (_config.GetBoolean("ssl"))
                builder.WithSSL();

            return builder;
        }

        protected virtual Task<IPEndPoint[]> LookupContactPoints(string clusterId)
        {
            var port = _config.GetInt("port");
            var contactPoints = _config.GetStringList("contact-points");
            return Task.FromResult(BuildContactPoints(contactPoints, port));
        }

        private IPEndPoint[] BuildContactPoints(IList<string> contactPoints, int port)
        {
            if (contactPoints == null || contactPoints.Count == 0)
                throw new ArgumentNullException(nameof(contactPoints), "A contact point list cannot be empty.");
            return contactPoints.Select(ipWithPort =>
            {
                var parts = ipWithPort.Split(':');
                if (parts.Length == 2)
                    return new IPEndPoint(IPAddress.Parse(parts[0]), int.Parse(parts[1]));
                if (parts.Length == 1)
                    return new IPEndPoint(IPAddress.Parse(parts[0]), port);
                throw new ArgumentException(
                    $"A contact point should have the form [host:port] or [host] but was: {ipWithPort}",
                    nameof(contactPoints));
            }).ToArray();
        }
    }
}
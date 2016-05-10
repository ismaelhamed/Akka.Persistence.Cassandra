using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests
{
    public class CassandraPluginConfigTest : Akka.TestKit.Xunit2.TestKit
    {
        public class TestContactPointsProvider : ConfigSessionProvider
        {
            public TestContactPointsProvider(ActorSystem system, Config config) : base(system, config)
            {
            }

            public override Task<IPEndPoint[]> LookupContactPoints(string clusterId)
            {
                if (clusterId == "cluster1")
                    return Task.FromResult(new[] {new IPEndPoint(IPAddress.Parse("123.123.231.241"), 9041)});
                return
                    Task.FromResult(new[]
                    {
                        new IPEndPoint(IPAddress.Parse("123.123.231.242"), 9041),
                        new IPEndPoint(IPAddress.Parse("123.123.231.243"), 9042)
                    });
            }
        }

        private readonly Lazy<Config> _defaultConfig;
        private readonly Lazy<IDictionary<string, bool>> _keyspaceNames;

        public CassandraPluginConfigTest(ITestOutputHelper output = null)
            : base(Config.Empty, "CassandraPluginConfigTest", output)
        {
            _defaultConfig =
                new Lazy<Config>(
                    () =>
                        ConfigurationFactory.FromResource<CassandraPluginConfig>(
                            "Akka.Persistence.Cassandra.reference.conf").GetConfig("cassandra-journal"));
            _keyspaceNames = new Lazy<IDictionary<string, bool>>(() =>
            {
                // Generate a key that is the max acceptable length ensuring the first char is alpha
                const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                var random = new Random();
                var maxKey = new string(Enumerable.Range(1, 48)
                    .Select(i => chars[random.Next(i == 1 ? chars.Length - 10 : chars.Length)]).ToArray());
                return new Dictionary<string, bool>
                {
                    {"test", true},
                    {"_test_123", false},
                    {"", false},
                    {"test-space", false},
                    {"'test'", false},
                    {"a", true},
                    {"a_", true},
                    {"1", false},
                    {"a1", true},
                    {"_", false},
                    {"asdf!", false},
                    {maxKey, true},
                    {"\"_asdf\"", false},
                    {"\"_\"", false},
                    {"\"a\"", true},
                    {"\"a_sdf\"", true},
                    {"\"\"", false},
                    {"\"valid_with_quotes\"", true},
                    {"\"missing_trailing_quote", false},
                    {"missing_leading_quote\"", false},
                    {'"' + maxKey + '"', true},
                    {maxKey + "_", false}
                };
            });
        }

        [Fact]
        public void A_CassadraPluginConfig_should_use_ConfigSessionProvider_by_default()
        {
            var config = new CassandraPluginConfig(Sys, _defaultConfig.Value);
            config.SessionProvider.Should().BeOfType<ConfigSessionProvider>();
        }

        [Fact]
        public void A_CassadraPluginConfig_should_set_the_fetch_size_to_the_max_result_size()
        {
            var config = new CassandraPluginConfig(Sys, _defaultConfig.Value);
            ((ConfigSessionProvider) config.SessionProvider).FetchSize.Should().Be(50001);
        }

        [Fact]
        public void A_CassadraPluginConfig_should_set_the_metadata_table()
        {
            var config = new CassandraPluginConfig(Sys, _defaultConfig.Value);
            config.MetadataTable.Should().Be("metadata");
        }

        [Fact]
        public void A_CassadraPluginConfig_should_parse_config_with_host_colon_port_values_as_contact_points()
        {
            var configWithHostPortPair =
                ConfigurationFactory.ParseString("contact-points = [\"127.0.0.1:19142\", \"127.0.0.2:29142\"]")
                    .WithFallback(_defaultConfig.Value);
            var config = new CassandraPluginConfig(Sys, configWithHostPortPair);
            var sessionProvider = (ConfigSessionProvider) config.SessionProvider;
            var contactPoints = Await.Result(sessionProvider.LookupContactPoints(""), TimeSpan.FromSeconds(3));
            contactPoints.Should().BeEquivalentTo(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 19142), new IPEndPoint(IPAddress.Parse("127.0.0.2"), 29142));
        }

        [Fact]
        public void A_CassadraPluginConfig_should_parse_config_with_a_list_of_contact_points_without_port()
        {
            var configWithHost =
                ConfigurationFactory.ParseString("contact-points = [\"127.0.0.1\", \"127.0.0.2\"]")
                    .WithFallback(_defaultConfig.Value);
            var config = new CassandraPluginConfig(Sys, configWithHost);
            var sessionProvider = (ConfigSessionProvider) config.SessionProvider;
            var contactPoints = Await.Result(sessionProvider.LookupContactPoints(""), TimeSpan.FromSeconds(3));
            contactPoints.Should().BeEquivalentTo(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042), new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042));
        }

        [Fact]
        public void A_CassadraPluginConfig_should_use_custom_ConfigSessionProvider_for_cluster1()
        {
            var configWithContactPointsProvider =
                ConfigurationFactory.ParseString(@"
session-provider = ""Akka.Persistence.Cassandra.Tests.CassandraPluginConfigTest+TestContactPointsProvider, Akka.Persistence.Cassandra.Tests""
cluster-id = cluster1
").WithFallback(_defaultConfig.Value);
            var config = new CassandraPluginConfig(Sys, configWithContactPointsProvider);
            var sessionProvider = (ConfigSessionProvider) config.SessionProvider;
            var contactPoints = Await.Result(sessionProvider.LookupContactPoints("cluster1"), TimeSpan.FromSeconds(3));
            contactPoints.Should().BeEquivalentTo(new IPEndPoint(IPAddress.Parse("123.123.231.241"), 9041));
        }

        [Fact]
        public void A_CassadraPluginConfig_should_use_custom_ConfigSessionProvider_for_cluster2()
        {
            var configWithContactPointsProvider =
                ConfigurationFactory.ParseString(@"
session-provider = ""Akka.Persistence.Cassandra.Tests.CassandraPluginConfigTest+TestContactPointsProvider, Akka.Persistence.Cassandra.Tests""
cluster-id = cluster2
").WithFallback(_defaultConfig.Value);
            var config = new CassandraPluginConfig(Sys, configWithContactPointsProvider);
            var sessionProvider = (ConfigSessionProvider) config.SessionProvider;
            var contactPoints = Await.Result(sessionProvider.LookupContactPoints("cluster2"), TimeSpan.FromSeconds(3));
            contactPoints.Should().BeEquivalentTo(new IPEndPoint(IPAddress.Parse("123.123.231.242"), 9041), new IPEndPoint(IPAddress.Parse("123.123.231.243"), 9042));
        }

        [Fact]
        public void A_CassadraPluginConfig_should_throw_an_exception_when_contact_point_list_is_empty()
        {
            var cfg = ConfigurationFactory.ParseString("contact-points = []").WithFallback(_defaultConfig.Value);
            var config = new CassandraPluginConfig(Sys, cfg);
            var sessionProvider = (ConfigSessionProvider) config.SessionProvider;
            sessionProvider.Invoking(p => Await.Result(p.LookupContactPoints("cluster2"), TimeSpan.FromSeconds(3)))
                .ShouldThrow<ArgumentException>();
        }

        [Fact]
        public void A_CassadraPluginConfig_should_parse_config_with_SimpleStrategy_as_default_for_replication_strategy()
        {
            var config = new CassandraPluginConfig(Sys, _defaultConfig.Value);
            config.ReplicationStrategy.Should().Be("'SimpleStrategy','replication_factor':1");
        }

        [Fact]
        public void A_CassadraPluginConfig_should_parse_config_with_a_list_datacenters_configured_for_NetworkTopologyStrategy()
        {
            var configWithNetworkStrategy = ConfigurationFactory.ParseString(@"
replication-strategy = ""NetworkTopologyStrategy""
data-center-replication-factors = [""dc1:3"", ""dc2:2""]
").WithFallback(_defaultConfig.Value);
            var config = new CassandraPluginConfig(Sys, configWithNetworkStrategy);
            config.ReplicationStrategy.Should().Be("'NetworkTopologyStrategy','dc1':3,'dc2':2");
        }

        [Fact]
        public void A_CassadraPluginConfig_should_throw_an_exception_for_an_unknown_replication_strategy()
        {
            this.Invoking(_ => CassandraPluginConfig.GetReplicationStrategy("UnknownStrategy", 0, new List<string>()))
                .ShouldThrow<ArgumentException>();
        }

        [Fact]
        public void
            A_CassadraPluginConfig_should_throw_an_exception_when_data_center_replication_factors_is_invalid_or_empty_for_NetworkTopologyStrategy
            ()
        {
            this.Invoking(
                _ => CassandraPluginConfig.GetReplicationStrategy("NetworkTopologyStrategy", 0, new List<string>()))
                .ShouldThrow<ArgumentException>();

            this.Invoking(_ => CassandraPluginConfig.GetReplicationStrategy("NetworkTopologyStrategy", 0, null))
                .ShouldThrow<ArgumentException>();

            this.Invoking(
                _ =>
                    CassandraPluginConfig.GetReplicationStrategy("NetworkTopologyStrategy", 0, new List<string> {"dc1"}))
                .ShouldThrow<ArgumentException>();
        }

        [Fact]
        public void A_CassadraPluginConfig_should_validate_keyspace_parameter()
        {
            foreach (var kvp in _keyspaceNames.Value)
            {
                if (kvp.Value)
                    CassandraPluginConfig.ValidateKeyspaceName(kvp.Key).Should().Be(kvp.Key);
                else
                    kvp.Key.Invoking(keyspace => CassandraPluginConfig.ValidateKeyspaceName(keyspace))
                        .ShouldThrow<ArgumentException>();
            }
        }

        [Fact]
        public void A_CassadraPluginConfig_should_validate_table_name_parameter()
        {
            foreach (var kvp in _keyspaceNames.Value)
            {
                if (kvp.Value)
                    CassandraPluginConfig.ValidateTableName(kvp.Key).Should().Be(kvp.Key);
                else
                    kvp.Key.Invoking(keyspace => CassandraPluginConfig.ValidateTableName(keyspace))
                        .ShouldThrow<ArgumentException>();
            }
        }

        [Fact]
        public void A_CassadraPluginConfig_should_parse_keyspace_autocreate_parameter()
        {
            var configWithFalseKeyspaceAutocreate =
                ConfigurationFactory.ParseString("keyspace-autocreate = false").WithFallback(_defaultConfig.Value);

            var config = new CassandraPluginConfig(Sys, configWithFalseKeyspaceAutocreate);
            config.KeyspaceAutocreate.Should().Be(false);
        }

        [Fact]
        public void A_CassadraPluginConfig_should_parse_tables_autocreate_parameter()
        {
            var configWithFalseTablesAutocreate =
                ConfigurationFactory.ParseString("tables-autocreate = false").WithFallback(_defaultConfig.Value);

            var config = new CassandraPluginConfig(Sys, configWithFalseTablesAutocreate);
            config.TablesAutocreate.Should().Be(false);
        }

        /*

    "parse keyspace-autocreate parameter" in {
      val configWithFalseKeyspaceAutocreate = ConfigFactory.parseString("""keyspace-autocreate = false""").withFallback(defaultConfig)

      val config = new CassandraPluginConfig(system, configWithFalseKeyspaceAutocreate)
      config.keyspaceAutoCreate must be(false)
    }

    "parse tables-autocreate parameter" in {
      val configWithFalseTablesAutocreate = ConfigFactory.parseString("""tables-autocreate = false""").withFallback(defaultConfig)

      val config = new CassandraPluginConfig(system, configWithFalseTablesAutocreate)
      config.tablesAutoCreate must be(false)
    }

    */
    }
}
using Akka.Configuration;
using Akka.Persistence.Cassandra.Compaction;
using Cassandra;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Compaction
{
    public class CassandraCompactionStrategySpec : CassandraPersistenceSpec
    {
        private new static readonly Config Config =
            ConfigurationFactory.ParseString(
                $@"
cassandra-journal.port = {CassandraConfig.Port}
cassandra-snapshot-store.port = {CassandraConfig.Port}
cassandra-journal.keyspace = CompactionStrategySpec
cassandra-snapshot-store.keyspace = CompactionStrategySpecSnapshot"
                ).WithFallback(CassandraPersistenceSpec.Config);

        private readonly ISession _session;

        public CassandraCompactionStrategySpec(ITestOutputHelper output = null)
            : base(Config, "CassandraCompactionStrategySpec", output)
        {
            var defaultConfigs = Sys.Settings.Config.GetConfig("cassandra-journal");
            var cassandraPluginConfig = new CassandraPluginConfig(Sys, defaultConfigs);

            var sessionTask = cassandraPluginConfig.SessionProvider.Connect();
            sessionTask.Wait(5000);
            _session = sessionTask.Result;

            _session.Execute("DROP KEYSPACE IF EXISTS testKeyspace");
            _session.Execute(
                "CREATE KEYSPACE IF NOT EXISTS testKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        }

        protected override void AfterAll()
        {
            _session.Dispose();
            _session.Cluster.Dispose();
            base.AfterAll();
        }

        [Fact]
        public void CassandraCompactionStrategy_must_successfully_create_a_DateTieredCompactionStrategy()
        {
            var uniqueConfig = ConfigurationFactory.ParseString(@"
table-compaction-strategy {
    class = ""DateTieredCompactionStrategy""
    enabled = true
    tombstone_compaction_interval = 86400
    tombstone_threshold = 0.1
    unchecked_tombstone_compaction = false
    base_time_seconds = 100
    max_sstable_age_days = 100
    max_threshold = 20
    min_threshold = 10
    timestamp_resolution = ""MICROSECONDS""
}"
                );

            var compactionStrategy =
                (DateTieredCompactionStrategy)
                    CassandraCompactionStrategy.Create(uniqueConfig.GetConfig("table-compaction-strategy"));

            compactionStrategy.Enabled.Should().Be(true);
            compactionStrategy.TombstoneCompactionInterval.Should().Be(86400);
            compactionStrategy.TombstoneThreshold.Should().Be(0.1);
            compactionStrategy.UncheckedTombstoneCompaction.Should().Be(false);
            compactionStrategy.BaseTimeSeconds.Should().Be(100);
            compactionStrategy.MaxSSTableAgeDays.Should().Be(100);
            compactionStrategy.MaxThreshold.Should().Be(20);
            compactionStrategy.MinThreshold.Should().Be(10);
            compactionStrategy.TimestampResolution.Should().Be("MICROSECONDS");
        }

        [Fact]
        public void CassandraCompactionStrategy_must_successfully_create_CQL_from_DateTieredCompactionStrategy()
        {
            var uniqueConfig = ConfigurationFactory.ParseString(@"
table-compaction-strategy {
    class = ""DateTieredCompactionStrategy""
    enabled = true
    tombstone_compaction_interval = 86400
    tombstone_threshold = 0.1
    unchecked_tombstone_compaction = false
    base_time_seconds = 100
    max_sstable_age_days = 100
    max_threshold = 20
    min_threshold = 10
    timestamp_resolution = ""MICROSECONDS""
}"
                );

            var cqlExpression =
                    CassandraCompactionStrategy.Create(uniqueConfig.GetConfig("table-compaction-strategy")).AsCQL;

            cqlExpression.Should().Be(@"{
'class' : 'DateTieredCompactionStrategy',
'enabled' : true,
'tombstone_compaction_interval' : 86400,
'tombstone_threshold' : 0.1,
'unchecked_tombstone_compaction' : false,
'base_time_seconds' : 100,
'max_sstable_age_days' : 100,
'max_threshold' : 20,
'min_threshold' : 10,
'timestamp_resolution' : 'MICROSECONDS'
}");

            cqlExpression.Invoking(
                cql =>
                    _session.Execute(
                        $"CREATE TABLE testKeyspace.testTable1 (testId TEXT PRIMARY KEY) WITH compaction = {cql}"))
                .ShouldNotThrow();
        }

        [Fact]
        public void CassandraCompactionStrategy_must_successfully_create_a_LeveledCompactionStrategy()
        {
            var uniqueConfig = ConfigurationFactory.ParseString(@"
table-compaction-strategy {
    class = ""LeveledCompactionStrategy""
    enabled = true
    tombstone_compaction_interval = 86400
    tombstone_threshold = 0.1
    unchecked_tombstone_compaction = false
    sstable_size_in_mb = 100
}"
                );

            var compactionStrategy =
                (LeveledCompactionStrategy)
                    CassandraCompactionStrategy.Create(uniqueConfig.GetConfig("table-compaction-strategy"));

            compactionStrategy.Enabled.Should().Be(true);
            compactionStrategy.TombstoneCompactionInterval.Should().Be(86400);
            compactionStrategy.TombstoneThreshold.Should().Be(0.1);
            compactionStrategy.UncheckedTombstoneCompaction.Should().Be(false);
            compactionStrategy.SSTableSizeInMb.Should().Be(100);
        }

        [Fact]
        public void CassandraCompactionStrategy_must_successfully_create_CQL_from_LeveledCompactionStrategy()
        {
            var uniqueConfig = ConfigurationFactory.ParseString(@"
table-compaction-strategy {
    class = ""LeveledCompactionStrategy""
    enabled = true
    tombstone_compaction_interval = 86400
    tombstone_threshold = 0.1
    unchecked_tombstone_compaction = false
    sstable_size_in_mb = 100
}"
                );

            var cqlExpression =
                    CassandraCompactionStrategy.Create(uniqueConfig.GetConfig("table-compaction-strategy")).AsCQL;

            cqlExpression.Should().Be(@"{
'class' : 'LeveledCompactionStrategy',
'enabled' : true,
'tombstone_compaction_interval' : 86400,
'tombstone_threshold' : 0.1,
'unchecked_tombstone_compaction' : false,
'sstable_size_in_mb' : 100
}");

            cqlExpression.Invoking(
                cql =>
                    _session.Execute(
                        $"CREATE TABLE testKeyspace.testTable1 (testId TEXT PRIMARY KEY) WITH compaction = {cql}"))
                .ShouldNotThrow();
        }

        [Fact]
        public void CassandraCompactionStrategy_must_successfully_create_a_SizeTieredCompactionStrategy()
        {
            var uniqueConfig = ConfigurationFactory.ParseString(@"
table-compaction-strategy {
    class = ""SizeTieredCompactionStrategy""
    enabled = true
    tombstone_compaction_interval = 86400
    tombstone_threshold = 0.1
    unchecked_tombstone_compaction = false
    bucket_high = 5.0
    bucket_low = 2.5 
    max_threshold = 20
    min_threshold = 10
    min_sstable_size = 100
}"
                );

            var compactionStrategy =
                (SizeTieredCompactionStrategy)
                    CassandraCompactionStrategy.Create(uniqueConfig.GetConfig("table-compaction-strategy"));

            compactionStrategy.Enabled.Should().Be(true);
            compactionStrategy.TombstoneCompactionInterval.Should().Be(86400);
            compactionStrategy.TombstoneThreshold.Should().Be(0.1);
            compactionStrategy.UncheckedTombstoneCompaction.Should().Be(false);
            compactionStrategy.BucketHigh.Should().Be(5.0);
            compactionStrategy.BucketLow.Should().Be(2.5);
            compactionStrategy.MaxThreshold.Should().Be(20);
            compactionStrategy.MinThreshold.Should().Be(10);
            compactionStrategy.MinSSTableSize.Should().Be(100);
        }

        [Fact]
        public void CassandraCompactionStrategy_must_successfully_create_CQL_from_SizeTieredCompactionStrategy()
        {
            var uniqueConfig = ConfigurationFactory.ParseString(@"
table-compaction-strategy {
    class = ""SizeTieredCompactionStrategy""
    enabled = true
    tombstone_compaction_interval = 86400
    tombstone_threshold = 0.1
    unchecked_tombstone_compaction = false
    bucket_high = 5.0
    bucket_low = 2.5 
    max_threshold = 20
    min_threshold = 10
    min_sstable_size = 100
}"
                );

            var cqlExpression =
                    CassandraCompactionStrategy.Create(uniqueConfig.GetConfig("table-compaction-strategy")).AsCQL;

            cqlExpression.Should().Be(@"{
'class' : 'SizeTieredCompactionStrategy',
'enabled' : true,
'tombstone_compaction_interval' : 86400,
'tombstone_threshold' : 0.1,
'unchecked_tombstone_compaction' : false,
'bucket_high' : 5,
'bucket_low' : 2.5,
'max_threshold' : 20,
'min_threshold' : 10,
'min_sstable_size' : 100
}");

            cqlExpression.Invoking(
                cql =>
                    _session.Execute(
                        $"CREATE TABLE testKeyspace.testTable1 (testId TEXT PRIMARY KEY) WITH compaction = {cql}"))
                .ShouldNotThrow();
        }
    }
}
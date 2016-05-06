using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Cassandra.Journal;
using Akka.Util.Internal;
using Cassandra;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class CassandraConfigCheckerSpec : CassandraPersistenceSpec
    {
        private static readonly string ConfigString =
            $@"
akka.persistence.journal.max-deletion-batch-size = 3
akka.persistence.publish-confirmations = on
akka.persistence.publish-plugin-commands = on
cassandra-journal.target-partition-size = 5
cassandra-journal.max-result-size = 3
cassandra-journal.port = {CassandraConfig.Port}
cassandra-snapshot-store.port = {CassandraConfig.Port}";

        private new static readonly Config Config =
            ConfigurationFactory.ParseString(ConfigString).WithFallback(CassandraPersistenceSpec.Config);

        internal class DummyActor : PersistentActor
        {
            public DummyActor(string persistenceId, IActorRef receiver)
            {
                PersistenceId = persistenceId;
                Receiver = receiver;
            }

            public override string PersistenceId { get; }
            public IActorRef Receiver { get; }

            protected override bool ReceiveRecover(object message)
            {
                return true;
            }

            protected override bool ReceiveCommand(object message)
            {
                Persist(message, msg => Receiver.Tell($"Received {message}"));
                return true;
            }
        }

        private readonly Config _cfg;
        private readonly CassandraPluginConfig _pluginConfig;
        private readonly Lazy<ISession> _session;

        public CassandraConfigCheckerSpec(ITestOutputHelper output = null) : base(Config, "CassandraConfigCheckerSpec", output)
        {
            _cfg = Sys.Settings.Config.GetConfig("cassandra-journal");
            _pluginConfig = new CassandraPluginConfig(Sys, _cfg);

            _session = new Lazy<ISession>(() =>
            {
                var sessionTask = _pluginConfig.SessionProvider.Connect();
                sessionTask.Wait(5000);
                return sessionTask.Result;
            });
        }

        protected override void AfterAll()
        {
            _session.Value.Dispose();
            base.AfterAll();
        }

        [Fact]
        public void CassandraConfigChecker_should_persist_value_in_cassandra()
        {
            WaitForPersistenceInitialization();
            var underTest = CreateCassandraConfigChecker(_cfg);
            _session.Value.Execute($"TRUNCATE {_pluginConfig.Keyspace}.{_pluginConfig.ConfigTable}");

            var persistentConfig = Await.Result(underTest.InitializePersistentConfig(_session.Value), RemainingOrDefault);
            persistentConfig.ContainsKey(CassandraJournalConfig.TargetPartitionProperty).Should().BeTrue();
            persistentConfig[CassandraJournalConfig.TargetPartitionProperty].Should().Be("5");
            GetTargetSize(underTest).Should().Be("5");
        }

        [Fact]
        public void CassandraConfigChecker_multiple_persistence_should_keep_the_same_value()
        {
            WaitForPersistenceInitialization();
            CreateCassandraConfigChecker(_cfg);
            _session.Value.Execute($"TRUNCATE {_pluginConfig.Keyspace}.{_pluginConfig.ConfigTable}");

            Enumerable.Range(1, 5).ForEach(_ =>
            {
                var underTest = CreateCassandraConfigChecker(_cfg);
                var persistentConfig = Await.Result(underTest.InitializePersistentConfig(_session.Value), RemainingOrDefault);
                persistentConfig.ContainsKey(CassandraJournalConfig.TargetPartitionProperty).Should().BeTrue();
                persistentConfig[CassandraJournalConfig.TargetPartitionProperty].Should().Be("5");
                GetTargetSize(underTest).Should().Be("5");
            });
        }

        [Fact]
        public void CassandraConfigChecker_should_throw_exception_when_starting_with_wrong_value()
        {
            WaitForPersistenceInitialization();
            var underTest = CreateCassandraConfigChecker(_cfg);
            _session.Value.Execute($"TRUNCATE {_pluginConfig.Keyspace}.{_pluginConfig.ConfigTable}");

            Await.Result(underTest.InitializePersistentConfig(_session.Value), RemainingOrDefault);

            var try3Size =
                CreateCassandraConfigChecker(
                    ConfigurationFactory.ParseString("target-partition-size = 3").WithFallback(_cfg));
            try3Size.Invoking(t => t.InitializePersistentConfig(_session.Value).Wait(RemainingOrDefault))
                .ShouldThrow<ArgumentException>();

            GetTargetSize(underTest).Should().Be("5");
        }

        [Fact]
        public void CassandraConfigChecker_concurrent_calls_should_keep_consistent_value()
        {
            WaitForPersistenceInitialization();
            _session.Value.Execute($"TRUNCATE {_pluginConfig.Keyspace}.{_pluginConfig.ConfigTable}");

            var result = Enumerable.Range(1, 10).Select(i =>
            {
                var underTest =
                    CreateCassandraConfigChecker(
                        ConfigurationFactory.ParseString($"target-partition-size = {i}").WithFallback(_cfg));
                return Tuple.Create(i, underTest.InitializePersistentConfig(_session.Value));
            }).ToList();

            Task.WhenAll(result.Select(r => r.Item2)).ContinueWith(t => true).Wait(5000);

            var firstSize = GetTargetSize(CreateCassandraConfigChecker(_cfg));

            var success = result.Where(r => r.Item1 == int.Parse(firstSize)).ToList();
            var failure = result.Where(r => r.Item1 != int.Parse(firstSize)).ToList();

            success.Count.Should().Be(1);
            success[0].Item2.Status.Should().Be(TaskStatus.RanToCompletion);
            success[0].Item2.Result[CassandraJournalConfig.TargetPartitionProperty].Should().Be(firstSize);

            failure.ForEach(f =>
            {
                f.Item2.Status.Should().Be(TaskStatus.Faulted);
                f.Item2.Exception.InnerExceptions.Count(e => e is ArgumentException).Should().BeGreaterOrEqualTo(1);
            });
        }

        private CassandraStatements CreateCassandraConfigChecker(Config cfg)
            => new CassandraStatements(new CassandraJournalConfig(Sys, cfg));

        private void WaitForPersistenceInitialization()
        {
            var actor = Sys.ActorOf(Props.Create(() => new DummyActor("p1", TestActor)));
            actor.Tell("Hi");
            ExpectMsg("Received Hi");
            actor.Tell(PoisonPill.Instance);
        }

        private string GetTargetSize(CassandraStatements checker)
        {
            return _session.Value.Execute(
                $"{checker.SelectConfig} WHERE property='{CassandraJournalConfig.TargetPartitionProperty}'")
                .First()
                .GetValue<string>("value");
        }
    }
}
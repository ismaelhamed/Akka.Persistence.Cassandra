using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Pattern;
using Akka.Util;
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    internal sealed class CassandraSession
    {
        private readonly ActorSystem _system;
        private readonly CassandraPluginConfig _settings;
        private readonly ILoggingAdapter _log;
        private readonly string _metricsCategory;
        private readonly Func<ISession, Task> _init;

        private readonly AtomicReference<Task<ISession>> _underlyingSession = new AtomicReference<Task<ISession>>();

        public CassandraSession(ActorSystem system, CassandraPluginConfig settings,
            ILoggingAdapter log, string metricsCategory, Func<ISession, Task> init)
        {
            _system = system;
            _settings = settings;
            _log = log;
            _metricsCategory = metricsCategory;
            _init = init;
        }

        public Task<ISession> Underlying => _underlyingSession.Value ?? Retry(Setup);

        /// <summary>
        /// This can only be used after successful initialization,
        /// otherwise throws <see cref="IllegalStateException"/>.
        /// </summary>
        public byte? ProtocolVersion
        {
            get
            {
                var underlying = Underlying;
                if (underlying.Status != TaskStatus.RanToCompletion)
                    throw new IllegalStateException("protocolVersion can only be accessed after successful init");

                return underlying.Result.Cluster.Configuration.ProtocolOptions.MaxProtocolVersion.Value;
            }
        }

        public async Task<PreparedStatement> Prepare(string statement)
        {
            var session = await Underlying;
            return await session.PrepareAsync(statement);
        }

        public async Task ExecuteWrite(Statement statement)
        {
            if (!statement.ConsistencyLevel.HasValue)
                statement.SetConsistencyLevel(_settings.WriteConsistency);
            var session = await Underlying;
            await session.ExecuteAsync(statement);
        }

        public async Task<RowSet> Select(Statement statement)
        {
            if (!statement.ConsistencyLevel.HasValue)
                statement.SetConsistencyLevel(_settings.ReadConsistency);
            var session = await Underlying;
            return await session.ExecuteAsync(statement);
        }

        public void Close()
        {
            var existing = _underlyingSession.GetAndSet(null);
            existing?.OnRanToCompletion(s => s.Dispose());
        }

        private Task<ISession> Setup()
        {
            var existing = _underlyingSession.Value;
            while (existing == null)
            {
                var session = Initialize(_settings.SessionProvider.Connect());
                if (_underlyingSession.CompareAndSet(null, session))
                {
                    // TODO metrics
                    //s.foreach { ses =>
                    //  CassandraMetricsRegistry(system).addMetrics(metricsCategory, ses.getCluster.getMetrics.getRegistry)
                    //}

                    session.OnFaultedOrCanceled(t =>
                    {
                        _underlyingSession.CompareAndSet(session, null);
                        _log.Warning(
                            $"Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {(t.IsFaulted ? t.Exception.Unwrap().Message : "task cancellation")}");
                    });
                    _system.RegisterOnTermination(() =>
                    {
                        session.OnRanToCompletion(s => s.Dispose());
                    });
                    existing = session;
                }
                else
                {
                    session.OnRanToCompletion(s => s.Dispose());
                    existing = _underlyingSession.Value;
                }
            }
            return existing;
        }

        private async Task<ISession> Initialize(Task<ISession> sessionTask)
        {
            var session = await sessionTask;
            try
            {
                await _init(session);
                return session;
            }
            catch (Exception)
            {
                Close(session);
                throw;
            }
        }

        private Task<ISession> Retry(Func<Task<ISession>> setup)
        {
            var promise = new TaskCompletionSource<ISession>();
            TrySetup(setup, promise, _settings.ConnectionRetries);
            return promise.Task;
        }

        private void TryAgain(Func<Task<ISession>> setup, TaskCompletionSource<ISession> promise, int count,
            Exception cause)
        {
            if (count == 0)
                promise.SetException(cause);
            else
            {
                _system.Scheduler.Advanced.ScheduleOnce(_settings.ConnectionRetryDelay,
                    () => TrySetup(setup, promise, count));
            }
        }

        private void TrySetup(Func<Task<ISession>> setup, TaskCompletionSource<ISession> promise, int count)
        {
            try
            {
                setup().ContinueWith(t =>
                {
                    if (!t.IsCanceled && !t.IsFaulted)
                        promise.SetResult(t.Result);
                    else
                    {
                        TryAgain(setup, promise, count - 1,
                            t.IsFaulted ? t.Exception.Unwrap() : new OperationCanceledException("Setup canceled, possibly due to timing out."));
                    }
                });
            }
            catch (Exception ex)
            {
                // this is only in case the direct calls, such as sessionProvider, throws
                _log.Warning(
                    $"Failed to initialize CassandraSession. It will be retried on demand. Caused by: {ex.Message}");
                promise.SetException(ex);
            }
        }

        private void Close(ISession session)
        {
            session.Dispose();
            session.Cluster.Dispose();
            // TODO CassandraMetricsRegistry(system).RemoveMetrics(_metricsCategory);
        }

        private static readonly AtomicReference<Task> SerializedExecutionProgress =
            new AtomicReference<Task>(Task.FromResult(new object()));

        internal static Task SerializedExecution(Func<Task> recur, Func<Task> exec)
        {
            var progress = SerializedExecutionProgress.Value;
            var promise = new TaskCompletionSource<object>();
            progress.ContinueWith(_ =>
            {
                var result = SerializedExecutionProgress.CompareAndSet(progress, promise.Task) ? exec() : recur();
                result.ContinueWith(t =>
                {
                    if (t.IsCanceled)
                        promise.SetCanceled();
                    else if (t.IsFaulted)
                        promise.SetException(t.Exception.Flatten().InnerExceptions);
                    else
                        promise.SetResult(new object());
                });
                return result;
            });
            return promise.Task;
        }
    }
}

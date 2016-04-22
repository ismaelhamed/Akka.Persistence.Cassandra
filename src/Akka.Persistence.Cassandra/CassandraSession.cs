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
        private readonly CassandraSettings _settings;
        private readonly ILoggingAdapter _log;
        private readonly string _metricsCategory;
        private readonly Func<ISession, Task> _init;

        private readonly AtomicReference<Task<ISession>> _underlyingSession = new AtomicReference<Task<ISession>>();

        public CassandraSession(ActorSystem system, CassandraSettings settings,
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

        public Task<PreparedStatement> Prepare(string statement)
        {
            return
                Underlying.ContinueWith(t => t.Result.PrepareAsync(statement),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                    .Unwrap();
        }

        public Task ExecuteWrite(Statement statement)
        {
            if (!statement.ConsistencyLevel.HasValue)
                statement.SetConsistencyLevel(_settings.WriteConsistency);
            return
                Underlying.ContinueWith(t => t.Result.ExecuteAsync(statement),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                    .Unwrap();
        }

        public Task<RowSet> Select(Statement statement)
        {
            if (!statement.ConsistencyLevel.HasValue)
                statement.SetConsistencyLevel(_settings.ReadConsistency);
            return
                Underlying.ContinueWith(t => t.Result.ExecuteAsync(statement),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                    .Unwrap();
        }

        public void Close()
        {
            var existing = _underlyingSession.GetAndSet(null);
            existing?.ContinueWith(t => t.Result.Dispose(), TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        private Task<ISession> Setup()
        {
            var existing = _underlyingSession.Value;
            while (existing == null)
            {
                var s = Initialize(_settings.SessionProvider.Connect());
                if (_underlyingSession.CompareAndSet(null, s))
                {
                    // TODO metrics
                    //s.foreach { ses =>
                    //  CassandraMetricsRegistry(system).addMetrics(metricsCategory, ses.getCluster.getMetrics.getRegistry)
                    //}

                    s.ContinueWith(t =>
                    {
                        _underlyingSession.CompareAndSet(s, null);
                        _log.Warning(
                            $"Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {(t.IsFaulted ? t.Exception.Message : "task cancellation")}");
                    }, TaskContinuationOptions.NotOnRanToCompletion);
                    _system.RegisterOnTermination(() =>
                    {
                        s.ContinueWith(t => t.Result.Dispose(), TaskContinuationOptions.OnlyOnRanToCompletion);
                    });
                    existing = s;
                }
                else
                {
                    s.ContinueWith(t => t.Result.Dispose(), TaskContinuationOptions.OnlyOnRanToCompletion);
                    existing = _underlyingSession.Value;
                }
            }
            return existing;
        }

        private Task<ISession> Initialize(Task<ISession> session)
        {
            return session.ContinueWith(t =>
            {
                var s = t.Result;
                var result = _init(t.Result);

                return result
                    .ContinueWith(_ => Close(s), TaskContinuationOptions.NotOnRanToCompletion)
                    .ContinueWith(_ => s, TaskContinuationOptions.OnlyOnRanToCompletion);
            }, TaskContinuationOptions.OnlyOnRanToCompletion)
                .Unwrap();
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
                    if (t.IsCompleted)
                        promise.SetResult(t.Result);
                    else
                    {
                        TryAgain(setup, promise, count - 1,
                            t.IsFaulted ? (Exception) t.Exception : new ApplicationException("Cancelled"));
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

        private static readonly AtomicReference<Task> serializedExecutionProgress =
            new AtomicReference<Task>(Task.FromResult(new object()));

        internal static Task SerializedExecution(Func<Task> recur, Func<Task> exec)
        {
            var progress = serializedExecutionProgress.Value;
            var promise = new TaskCompletionSource<object>();
            progress.ContinueWith(t =>
            {
                var result = serializedExecutionProgress.CompareAndSet(progress, promise.Task) ? exec() : recur();
                promise.SetResult(result);
                return result;
            }, TaskContinuationOptions.OnlyOnRanToCompletion);
            return promise.Task;
        }
    }
}

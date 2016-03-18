//-----------------------------------------------------------------------
// <copyright file="QueryActorPublisher.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Actors;
using Cassandra;
using Continue = Akka.Persistence.Cassandra.Query.QueryActorPublisher.Continue;
using FetchedResultSet = Akka.Persistence.Cassandra.Query.QueryActorPublisher.FetchedResultSet;
using Finished = Akka.Persistence.Cassandra.Query.QueryActorPublisher.Finished;
using IAction = Akka.Persistence.Cassandra.Query.QueryActorPublisher.IAction;
using NewResultSet = Akka.Persistence.Cassandra.Query.QueryActorPublisher.NewResultSet;

namespace Akka.Persistence.Cassandra.Query
{

    #region Internal messages

    internal static class QueryActorPublisher
    {
        internal interface IAction : INoSerializationVerificationNeeded
        {
        }

        internal sealed class NewResultSet : IAction
        {
            public RowSet ResultSet { get; }

            public NewResultSet(RowSet resultSet)
            {
                ResultSet = resultSet;
            }
        }

        internal sealed class FetchedResultSet : IAction
        {
            public RowSet ResultSet { get; }

            public FetchedResultSet(RowSet resultSet)
            {
                ResultSet = resultSet;
            }
        }

        internal sealed class Finished : IAction
        {
            public RowSet ResultSet { get; }

            public Finished(RowSet resultSet)
            {
                ResultSet = resultSet;
            }
        }

        internal sealed class Continue
        {
            public static readonly Continue Instance = new Continue();

            private Continue()
            {
            }
        }
    }

    #endregion

    /// <summary>
    /// Abstract Query publisher. Can be integrated with Akka Streams as a Source.
    /// Intended to be extended by concrete Query publisher classes. This class manages the stream
    /// lifecycle, live stream updates, refreshInterval, max buffer size and causal consistency given an
    /// offset queryable data source.
    /// </summary>
    /// <typeparam name="TMessage">Type of message</typeparam>
    /// <typeparam name="TState">Type of state</typeparam>
    internal abstract class QueryActorPublisher<TMessage, TState> : ActorPublisher<TMessage>
    {
        #region Internal messages

        private interface IInitialAction : INoSerializationVerificationNeeded
        {
        }

        private sealed class InitialNewResultSet : IInitialAction
        {
            public InitialNewResultSet(TState state, RowSet resultSet)
            {
                State = state;
                ResultSet = resultSet;
            }

            public TState State { get; }
            public RowSet ResultSet { get; }
        }

        private sealed class InitialFinished : IInitialAction
        {
            public InitialFinished(TState state, RowSet resultSet)
            {
                State = state;
                ResultSet = resultSet;
            }

            public TState State { get; }
            public RowSet ResultSet { get; }
        }

        #endregion

        // TODO Handle database timeout, retry and failure handling.
        // TODO Write tests for buffer size, delivery buffer etc.

        private readonly ICancelable _tickTask;

        internal QueryActorPublisher(TimeSpan? refreshInterval, CassandraReadJournalConfig config)
        {
            RefreshInterval = refreshInterval;
            Config = config;

            if (refreshInterval.HasValue)
                _tickTask = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(refreshInterval.Value,
                    refreshInterval.Value, Self, Continue.Instance, Self);

            Become(Starting());
        }

        public TimeSpan? RefreshInterval { get; }
        public CassandraReadJournalConfig Config { get; }

        protected override void PostStop()
        {
            _tickTask?.Cancel();
            base.PostStop();
        }

        protected override void PreStart()
        {
            RunInitialQuery().PipeTo(Self);
        }

        private async Task<IInitialAction> RunInitialQuery()
        {
            var initialState = await InitialState();
            var action = await InitialQuery(initialState);
            if (action is NewResultSet)
            {
                return new InitialNewResultSet(initialState, ((NewResultSet) action).ResultSet);
            }
            if (action is FetchedResultSet)
            {
                return new InitialNewResultSet(initialState, ((FetchedResultSet) action).ResultSet);
            }
            if (action is Finished)
            {
                return new InitialFinished(initialState, ((Finished) action).ResultSet);
            }
            throw new ApplicationException("Should never happen");
        }

        private Receive Starting()
        {
            return message =>
            {
                if (message is Cancel || message is SubscriptionTimeoutExceeded)
                {
                    Context.Stop(Self);
                }
                else if (message is InitialNewResultSet)
                {
                    var initialNewResultSet = (InitialNewResultSet) message;
                    Context.Become(ExhaustFetchAndBecome(initialNewResultSet.ResultSet, initialNewResultSet.State, false,
                        false));
                }
                else if (message is InitialFinished)
                {
                    var initialNewResultSet = (InitialFinished) message;
                    Context.Become(ExhaustFetchAndBecome(initialNewResultSet.ResultSet, initialNewResultSet.State, true,
                        false));
                }
                else return false;
                return true;
            };
        }

        private Receive Awaiting(RowSet resultSet, TState state, bool finished)
        {
            return message =>
            {
                if (message is Cancel || message is SubscriptionTimeoutExceeded)
                {
                    Context.Stop(Self);
                }
                else if (message is Request)
                {
                    Context.Become(ExhaustFetchAndBecome(resultSet, state, finished, false, Awaiting));
                }
                else if (message is NewResultSet)
                {
                    Context.Become(ExhaustFetchAndBecome(((NewResultSet) message).ResultSet, state, finished, false));
                }
                else if (message is FetchedResultSet)
                {
                    Context.Become(ExhaustFetchAndBecome(((FetchedResultSet) message).ResultSet, state, finished, false));
                }
                else if (message is Finished)
                {
                    Context.Become(ExhaustFetchAndBecome(((Finished) message).ResultSet, state, true, false));
                }
                else return false;
                return true;
            };
        }

        private Receive Idle(RowSet resultSet, TState state, bool finished)
        {
            return message =>
            {
                if (message is Cancel || message is SubscriptionTimeoutExceeded)
                {
                    Context.Stop(Self);
                }
                else if (message is Request)
                {
                    Context.Become(ExhaustFetchAndBecome(resultSet, state, finished, false));
                }
                else if (message is Continue)
                {
                    Context.Become(ExhaustFetchAndBecome(resultSet, state, finished, true));
                }
                else return false;
                return true;
            };
        }

        protected override bool Receive(object message)
        {
            return false;
        }

        private Receive ExhaustFetchAndBecome(RowSet resultSet, TState state, bool finished, bool @continue,
            Func<RowSet, TState, bool, Receive> behavior = null)
        {
            var newResultSetAndNewState = ExhaustFetch(resultSet, state, resultSet.GetAvailableWithoutFetching(), 0,
                TotalDemand);
            var newResultSet = newResultSetAndNewState.Item1;
            var newState = newResultSetAndNewState.Item2;

            return behavior != null
                ? behavior(newResultSet, newState, finished)
                : NextBehavior(newResultSet, newState, finished, @continue);
        }

        // TODO Optimize
        private Receive NextBehavior(RowSet resultSet, TState state, bool finished, bool @continue)
        {
            var availableWithoutFetching = resultSet.GetAvailableWithoutFetching();
            var isFullyFetched = resultSet.IsFullyFetched;

            if (ShouldFetchMore(availableWithoutFetching, isFullyFetched, state))
            {
                FetchMoreResults(resultSet).PipeTo(Self);
                return Awaiting(resultSet, state, finished);
            }

            if (ShouldIdle(availableWithoutFetching, state))
                return Idle(resultSet, state, finished);

            var exhausted = IsExhausted(resultSet);

            if (ShouldComplete(exhausted, RefreshInterval, state, finished))
            {
                OnCompleteThenStop();
                return EmptyReceive;
            }
            if (ShouldRequestMore(exhausted, state, finished, @continue))
            {
                if (finished) RequestNextFinished(state, resultSet).PipeTo(Self);
                else RequestNext(state, resultSet).PipeTo(Self);
                return Awaiting(resultSet, state, finished);
            }
            return Idle(resultSet, state, finished);
        }

        private static async Task<FetchedResultSet> FetchMoreResults(RowSet resultSet)
        {
            await resultSet.FetchMoreResultsAsync();
            return new FetchedResultSet(resultSet);
        }

        private bool ShouldIdle(int availableWithoutFetching, TState state)
        {
            return availableWithoutFetching > 0 && !CompletionCondition(state);
        }

        private bool ShouldFetchMore(int availableWithoutFetching, bool isFullyFetched, TState state)
        {
            return !isFullyFetched &&
                   (availableWithoutFetching + Config.FetchSize <= Config.MaxBufferSize
                    || availableWithoutFetching == 0) &&
                   !CompletionCondition(state);
        }

        private bool ShouldRequestMore(bool isExhausted, TState state, bool finished, bool @continue)
        {
            return (!CompletionCondition(state) || RefreshInterval.HasValue) &&
                   !(finished && !@continue) &&
                   isExhausted;
        }

        private bool ShouldComplete(bool isExhausted, TimeSpan? refreshInterval, TState state, bool finished)
        {
            return (finished && !refreshInterval.HasValue && isExhausted) || CompletionCondition(state);
        }

        // ResultSet methods isExhausted(), one() etc. cause blocking database fetch if there aren't
        // any available items in the ResultSet buffer and it is not the last fetch batch
        // so we need to avoid calling them unless we know it won't block. See e.g. ArrayBackedResultSet.
        private bool IsExhausted(RowSet resultSet)
        {
            return resultSet.IsExhausted();
        }

        protected Tuple<RowSet, TState> ExhaustFetch(RowSet resultSet, TState state, int available, long count, long max)
        {
            while (available != 0 && count != max && !CompletionCondition(state))
            {
                var eventAndNextState = UpdateState(state, resultSet.FirstOrDefault());
                var @event = eventAndNextState.Item1;
                var nextState = eventAndNextState.Item2;
                if (@event != null)
                    OnNext(@event);
                state = nextState;
                available -= 1;
                count += 1;
            }
            return Tuple.Create(resultSet, state);
        }

        protected abstract Task<TState> InitialState();
        protected abstract Task<IAction> InitialQuery(TState initialState);
        protected abstract Task<IAction> RequestNext(TState state, RowSet resultSet);
        protected abstract Task<IAction> RequestNextFinished(TState state, RowSet resultSet);
        protected abstract Tuple<TMessage, TState> UpdateState(TState state, Row row);
        protected abstract bool CompletionCondition(TState state);
    }
}
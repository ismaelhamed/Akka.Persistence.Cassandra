using System;
using System.Collections.Generic;
using Akka.Actor;

namespace Akka.Persistence.Cassandra.Journal
{
    internal class PubSubThrotler : ActorBase
    {
        public static Props Props(IActorRef @delegate, TimeSpan interval)
        {
            return Actor.Props.Create(() => new PubSubThrotler(@delegate, interval));
        }

        private sealed class Tick
        {
            public static readonly Tick Instance = new Tick();
            private Tick() { }
        }

        // the messages we've already seen during this interval
        private readonly ISet<object> _seen;

        // the messages we've seen more than once during this interval, and their sender(s).
        private readonly IDictionary<object, ISet<IActorRef>> _repeated;

        private readonly ICancelable _timer;

        public PubSubThrotler(IActorRef @delegate, TimeSpan interval)
        {
            Delegate = @delegate;
            Interval = interval;

            _seen = new HashSet<object>();
            _repeated = new Dictionary<object, ISet<IActorRef>>();
            _timer = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(interval, interval, Self, Tick.Instance,
                Self);
        }

        public IActorRef Delegate { get; }
        public TimeSpan Interval { get; }

        protected override bool Receive(object message)
        {
            if (message is Tick)
            {
                foreach (var messageAndClients in _repeated)
                {
                    foreach (var client in messageAndClients.Value)
                    {
                        Delegate.Tell(messageAndClients.Key, client);
                    }
                }
                _seen.Clear();
                _repeated.Clear();
            }
            else
            {
                if (_seen.Contains(message))
                {
                    ISet<IActorRef> clients;
                    if (!_repeated.TryGetValue(message, out clients))
                    {
                        clients = new HashSet<IActorRef>();
                        _repeated.Add(message, clients);
                    }
                    clients.Add(Sender);
                }
                else
                {
                    Delegate.Forward(message);
                    _seen.Add(message);
                }
            }
            return true;
        }

        protected override void PostStop()
        {
            _timer.Cancel();
            base.PostStop();
        }
    }
}
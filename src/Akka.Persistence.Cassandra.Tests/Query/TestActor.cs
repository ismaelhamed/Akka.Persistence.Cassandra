//-----------------------------------------------------------------------
// <copyright file="TestActor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class TestActor : PersistentActor
    {
        public static Props Props(string persistenceId) =>
            Actor.Props.Create(() => new TestActor(persistenceId));

        public TestActor(string persistenceId)
        {
            PersistenceId = persistenceId;
        }

        public override string PersistenceId { get; }

        protected override bool ReceiveRecover(object message) => message is string;

        protected override bool ReceiveCommand(object message)
        {
            if (message is string)
            {
                Persist(message, evt => Sender.Tell(evt + "-done"));
                return true;
            }
            return false;
        }
    }
}
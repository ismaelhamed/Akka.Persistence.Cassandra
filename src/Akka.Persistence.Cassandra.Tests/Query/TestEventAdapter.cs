//-----------------------------------------------------------------------
// <copyright file="TestEventAdapter.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Persistence.Journal;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class TestEventAdapter : IEventAdapter
    {
        public string Manifest(object @event) => "";

        public object ToJournal(object @event)
        {
            if (@event is string && ((string) @event).StartsWith("tagged:"))
            {
                var parts = ((string) @event).Split(':');
                var tags = parts[1].Split(',').ToImmutableHashSet();
                var payload = string.Join(":", parts.Skip(2));
                return new Tagged(payload, tags);
            }
            return @event;
        }

        public IEventSequence FromJournal(object @event, string manifest)
        {
            if (@event is string && ((string) @event).Contains(":"))
            {
                var parts = ((string) @event).Split(':');
                if (parts[0].Equals("dropped"))
                    return EventSequence.Empty;
                if (parts[0].Equals("duplicated"))
                    return EventSequence.Create(parts[1], parts[1]);
                if (parts[0].Equals("prefixed"))
                    return EventSequence.Single($"{parts[1]}-{parts[2]}");
                throw new ArgumentException((string) @event);
            }
            return EventSequence.Single(@event);
        }
    }
}
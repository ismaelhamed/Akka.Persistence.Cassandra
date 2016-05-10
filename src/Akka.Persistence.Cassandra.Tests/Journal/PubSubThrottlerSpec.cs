//-----------------------------------------------------------------------
// <copyright file="PubSubThrottlerSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Persistence.Cassandra.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Cassandra.Tests.Journal
{
    public class PubSubThrottlerSpec : Akka.TestKit.Xunit2.TestKit
    {
        public PubSubThrottlerSpec(ITestOutputHelper output = null) : base(ActorSystem.Create("PubSubThrottlerSpec", DefaultConfig), output)
        {
        }

        [Fact]
        public void PubSubThrottler_should_eat_up_duplicate_messages_that_arrive_within_the_same_interval_window()
        {
            var @delegate = CreateTestProbe();
            var throttler = Sys.ActorOf(Props.Create(() => new PubSubThrottler(@delegate.Ref, TimeSpan.FromSeconds(5))));

            throttler.Tell("hello");
            throttler.Tell("hello");
            throttler.Tell("hello");
            @delegate.Within(TimeSpan.FromSeconds(2), () =>
            {
                @delegate.ExpectMsg("hello");
            });
            // Only first "hello" makes it through during the first interval.
            @delegate.ExpectNoMsg(TimeSpan.FromSeconds(2));

            // Eventually, the interval will roll over and forward ONE further hello.
            @delegate.ExpectMsg("hello", TimeSpan.FromSeconds(10));
            @delegate.ExpectNoMsg(TimeSpan.FromSeconds(2));

            throttler.Tell("hello");
            @delegate.Within(TimeSpan.FromSeconds(2), () =>
            {
                @delegate.ExpectMsg("hello");
            });
        }

        [Fact]
        public void PubSubThrottler_should_allow_differing_messages_to_pass_through_within_same_interval_window()
        {
            var @delegate = CreateTestProbe();
            var throttler = Sys.ActorOf(Props.Create(() => new PubSubThrottler(@delegate.Ref, TimeSpan.FromSeconds(5))));

            throttler.Tell("hello");
            throttler.Tell("world");
            @delegate.Within(TimeSpan.FromSeconds(2), () =>
            {
                @delegate.ExpectMsg("hello");
                @delegate.ExpectMsg("world");
            });
        }
    }
}
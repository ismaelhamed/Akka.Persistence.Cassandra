//-----------------------------------------------------------------------
// <copyright file="Await.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading.Tasks;

namespace Akka.Persistence.Cassandra.Tests
{
    public class Await
    {
        public static T Result<T>(Task<T> task, TimeSpan timeout)
        {
            task.Wait(timeout);
            return task.Result;
        }

        public static T Result<T>(Task<T> task, double timeoutMilliseconds)
        {
            return Result(task, TimeSpan.FromMilliseconds(timeoutMilliseconds));
        }
    }
}
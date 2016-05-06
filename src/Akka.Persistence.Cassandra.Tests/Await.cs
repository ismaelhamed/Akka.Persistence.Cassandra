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
//-----------------------------------------------------------------------
// <copyright file="CassandraCompactionStrategy.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    public interface ICassandraCompactionStrategy
    {
        string AsCql { get; }
    }

    public static class CassandraCompactionStrategy
    {
        public static ICassandraCompactionStrategy Create(Config config)
        {
            return BaseCompactionStrategy.FromConfig(config);
        }
    }
}
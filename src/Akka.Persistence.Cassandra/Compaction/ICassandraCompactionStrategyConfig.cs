//-----------------------------------------------------------------------
// <copyright file="ICassandraCompactionStrategyConfig.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Immutable;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra.Compaction
{
    public interface ICassandraCompactionStrategyConfig<out TStrategy> where TStrategy : ICassandraCompactionStrategy
    {
        string TypeName { get; }
        IImmutableList<string> PropertyKeys { get; }
        TStrategy FromConfig(Config config);
    }
}
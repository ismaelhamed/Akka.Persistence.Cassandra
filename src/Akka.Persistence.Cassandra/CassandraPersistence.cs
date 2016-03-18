//-----------------------------------------------------------------------
// <copyright file="CassandraPersistence.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;
using Akka.Configuration;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// Extension Id provider for the Cassandra Persistence extension.
    /// </summary>
    public class CassandraPersistence : ExtensionIdProvider<CassandraExtension>
    {
        public static readonly CassandraPersistence Instance = new CassandraPersistence();
        
        public override CassandraExtension CreateExtension(ExtendedActorSystem system)
        {
            return new CassandraExtension(system);
        }

        public static Config DefaultConfig()
        {
            return ConfigurationFactory.FromResource<CassandraPersistence>("Akka.Persistence.Cassandra.reference.conf");
        }
    }
}

//-----------------------------------------------------------------------
// <copyright file="CassandraConfig.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Configuration;

namespace Akka.Persistence.Cassandra.Tests
{
    public class CassandraConfig
    {
        static CassandraConfig()
        {
            var portString = ConfigurationManager.AppSettings["cassandra.port"];
            Port = string.IsNullOrWhiteSpace(portString) ? 9042 : int.Parse(portString);
        }
        public static int Port { get; }
    }
}
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
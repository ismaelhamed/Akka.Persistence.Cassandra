using System;
using Akka.Actor;
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// Extension methods used by the Cassandra persistence plugin.
    /// </summary>
    internal static class ExtensionMethods
    {
        /// <summary>
        /// Gets the PersistenceExtension instance registered with the ActorSystem. Throws an InvalidOperationException if not found.
        /// </summary>
        internal static PersistenceExtension PersistenceExtension(this ActorSystem system)
        {
            var ext = system.GetExtension<PersistenceExtension>();
            if (ext == null)
                throw new InvalidOperationException("Persistence extension not found.");

            return ext;
        }

        /// <summary>
        /// Converts a Type to a string representation that can be stored in Cassandra.
        /// </summary>
        internal static string ToQualifiedString(this Type t)
        {
            return $"{t.FullName}, {t.Assembly.GetName().Name}";
        }

        /// <summary>
        /// Prepares a CQL string with format arguments using the session.
        /// </summary>
        internal static PreparedStatement PrepareFormat(this ISession session, string cqlFormatString,
            params object[] args)
        {
            return session.Prepare(string.Format(cqlFormatString, args));
        }

        private static readonly byte[] MinNodeId = {0x80, 0x80, 0x80, 0x80, 0x80, 0x80};
        private static readonly byte[] MinClockId = {0x80, 0x80};
        private static readonly byte[] MaxNodeId = {0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f};
        private static readonly byte[] MaxClockId = {0x7f, 0x7f};

        public static TimeUuid StartOf(this DateTimeOffset date)
        {
            return TimeUuid.NewId(MinNodeId, MinClockId, date);
        }

        public static TimeUuid EndOf(this DateTimeOffset date)
        {
            return TimeUuid.NewId(MaxNodeId, MaxClockId, date);
        }
    }
}
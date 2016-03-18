//-----------------------------------------------------------------------
// <copyright file="ISessionProvider.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// <para>
    /// The implementation of the <see cref="ISessionProvider"/> is used for creating the
    /// Cassandra Session. By default the <see cref="ConfigSessionProvider"/> is building
    /// the Cluster from configuration properties but it is possible to
    /// replace the implementation of the SessionProvider to reuse another
    /// session or override the Cluster builder with other settings.
    /// </para>
    /// <para>
    /// The implementation is defined in configuration `session-provider` property.
    /// It may optionally have a constructor with an ActorSystem and Config parameter.
    /// The config parameter is the config section of the plugin.
    /// </para>
    /// </summary>
    public interface ISessionProvider
    {
        Task<ISession> Connect();
    }
}
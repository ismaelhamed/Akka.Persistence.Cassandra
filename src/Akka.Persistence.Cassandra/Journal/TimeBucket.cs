//-----------------------------------------------------------------------
// <copyright file="TimeBucket.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Cassandra;

namespace Akka.Persistence.Cassandra.Journal
{
    public class TimeBucket
    {
        public TimeBucket(LocalDate day, string key)
        {
            Day = day;
            Key = key;
        }

        public TimeBucket(string key) : this(ParseLocalDate(key), key)
        {
        }

        public TimeBucket(TimeUuid timeUuid): this(timeUuid.GetDate())
        {
        }

        public TimeBucket(DateTimeOffset dateTimeOffset): this(ToLocalDate(dateTimeOffset))
        {
        }

        public TimeBucket(long ticks) : this(ToLocalDate(ticks))
        {
        }

        public TimeBucket(LocalDate day) : this(day, $"{day.Year:D4}{day.Month:D2}{day.Day:D2}")
        {
        }

        public LocalDate Day { get; }
        public string Key { get; }

        public TimeBucket Next()
        {
            return new TimeBucket(Day.ToDateTimeOffset().AddDays(1));
        }

        public bool IsBefore(LocalDate other)
        {
            return Day < other;
        }

        public long StartTicks => Day.ToDateTimeOffset().Ticks;

        public override string ToString()
        {
            return Key;
        }

        private static LocalDate ParseLocalDate(string s)
        {
            return new LocalDate(int.Parse(s.Substring(0, 4)), int.Parse(s.Substring(4, 2)), int.Parse(s.Substring(6, 2)));
        }

        private static LocalDate ToLocalDate(long ticks)
        {
            var date = new DateTime(ticks, DateTimeKind.Utc);
            return new LocalDate(date.Year, date.Month, date.Day);
        }

        private static LocalDate ToLocalDate(DateTimeOffset dateTimeOffset)
        {
            return new LocalDate(dateTimeOffset.Year, dateTimeOffset.Month, dateTimeOffset.Day);
        }
    }
}
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    public class TimeBucket
    {
        private readonly string _key;
        private const string TimeBucketFormatter = "yyyyMMdd";
        public TimeBucket(TimeUuid timeUuid)
        {
            _key = timeUuid.GetDate().ToString(TimeBucketFormatter);
        }

        public string Key => _key;

        public override string ToString()
        {
            return _key;
        }
    }
}
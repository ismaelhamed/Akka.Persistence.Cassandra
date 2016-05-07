using Akka.Persistence.Cassandra.Query;
using FluentAssertions;
using Xunit;
using Answer = Akka.Persistence.Cassandra.Query.SequenceNumbers.Answer;

namespace Akka.Persistence.Cassandra.Tests.Query
{
    public class SequenceNumbersSpec
    {
        [Fact]
  public void SequenceNumbers_must_answer_IsNext()
        {
            var seqNumbers = SequenceNumbers.Empty;
            seqNumbers.IsNext("p1", 1L).Should().Be(Answer.Yes);
            seqNumbers.IsNext("p1", 2L).Should().Be(Answer.PossiblyFirst);

            var seqNumbers2 = seqNumbers.Updated("p1", 1L);
            seqNumbers2.IsNext("p1", 2L).Should().Be(Answer.Yes);
            seqNumbers2.IsNext("p1", 1L).Should().Be(Answer.Before);
            seqNumbers2.IsNext("p1", 3L).Should().Be(Answer.After);
            seqNumbers2.IsNext("p1", 1000L).Should().Be(Answer.After);
            seqNumbers2.IsNext("p2", 1L).Should().Be(Answer.Yes);
        }

        [Fact]
        public void SequenceNumbers_must_keep_current()
        {
            var seqNumbers = SequenceNumbers.Empty.Updated("p1", 1L).Updated("p2", 10L);
            seqNumbers.Get("p1").Should().Be(1L);
            seqNumbers.Get("p2").Should().Be(10L);
            seqNumbers.Get("p3").Should().Be(0L);
        }

        [Fact]
        public void SequenceNumbers_must_handle_updates_around_int_Maxvarue()
        {
            const long n = (long) (int.MaxValue - 1);
            var seqNumbers = SequenceNumbers.Empty.Updated("p1", n);
            seqNumbers.IsNext("p1", n + 1).Should().Be(Answer.Yes);
            seqNumbers.IsNext("p1", n).Should().Be(Answer.Before);
            seqNumbers.IsNext("p1", n + 10).Should().Be(Answer.After);

            var seqNumbers2 = seqNumbers.Updated("p1", n + 1);
            seqNumbers2.IsNext("p1", n + 2).Should().Be(Answer.Yes);
            seqNumbers2.IsNext("p1", n).Should().Be(Answer.Before);
            seqNumbers2.IsNext("p1", n + 1).Should().Be(Answer.Before);
            seqNumbers2.IsNext("p1", n + 10).Should().Be(Answer.After);

            var seqNumbers3 = seqNumbers.Updated("p1", n + 2);
            seqNumbers3.IsNext("p1", n + 3).Should().Be(Answer.Yes);
            seqNumbers3.IsNext("p1", n).Should().Be(Answer.Before);
            seqNumbers3.IsNext("p1", n + 1).Should().Be(Answer.Before);
            seqNumbers3.IsNext("p1", n + 2).Should().Be(Answer.Before);
            seqNumbers3.IsNext("p1", n + 10).Should().Be(Answer.After);
        }
    }
}
/* (C)2023 */
package org.example;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.datatypes.TaxiFare;
import org.example.sources.TaxiFareGenerator;
import org.junit.Test;
import testing.ComposedPipeline;
import testing.ExecutablePipeline;
import testing.TestSink;

public class HourlyTipsTableTest extends HourlyTipsTest {

  @Test
  public void testWithDataGenerator() throws Exception {

    // the TaxiFareGenerator is deterministic, and will produce these results if the
    // watermarking doesn't produce late events
    TaxiFareGenerator source = TaxiFareGenerator.runFor(Duration.ofMinutes(180));
    Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 2013000089L, 76.0F);
    Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 2013000197L, 71.0F);
    Tuple3<Long, Long, Float> hour3 = Tuple3.of(t(180).toEpochMilli(), 2013000118L, 83.0F);

    assertThat(results(source)).containsExactlyInAnyOrder(hour1, hour2, hour3);
  }

  private static final ExecutablePipeline<TaxiFare, Tuple3<Long, Long, Float>> exercise =
      (source, sink) -> new HourlyTipsTableExercise(source, sink).execute();

  private static final ExecutablePipeline<TaxiFare, Tuple3<Long, Long, Float>> solution =
      (source, sink) -> new HourlyTipsTableSolution(source, sink).execute();

  private static final ComposedPipeline<TaxiFare, Tuple3<Long, Long, Float>> testPipeline =
      new ComposedPipeline<>(exercise, solution);

  protected List<Tuple3<Long, Long, Float>> results(SourceFunction<TaxiFare> source)
      throws Exception {

    TestSink<Tuple3<Long, Long, Float>> sink = new TestSink<>();
    JobExecutionResult jobResult = testPipeline.execute(source, sink);
    return sink.getResults(jobResult);
  }
}

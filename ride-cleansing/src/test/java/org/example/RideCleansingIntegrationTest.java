/* (C)2023 */
package org.example;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.example.datatypes.TaxiRide;
import org.junit.ClassRule;
import org.junit.Test;
import testing.ComposedPipeline;
import testing.ExecutablePipeline;
import testing.ParallelTestSource;
import testing.TestSink;

public class RideCleansingIntegrationTest extends RideCleansingTestBase {

  private static final int PARALLELISM = 2;

  /** This isn't necessary, but speeds up the tests. */
  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(PARALLELISM)
              .setNumberTaskManagers(1)
              .build());

  @Test
  public void testAMixtureOfLocations() throws Exception {

    TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
    TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
    TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
    TaxiRide atNorthPole = testRide(0, 90, 0, 90);

    ParallelTestSource<TaxiRide> source =
        new ParallelTestSource<>(toThePole, fromThePole, atPennStation, atNorthPole);
    TestSink<TaxiRide> sink = new TestSink<>();

    JobExecutionResult jobResult = rideCleansingPipeline().execute(source, sink);
    assertThat(sink.getResults(jobResult)).containsExactly(atPennStation);
  }

  protected ComposedPipeline<TaxiRide, TaxiRide> rideCleansingPipeline() {

    ExecutablePipeline<TaxiRide, TaxiRide> exercise =
        (source, sink) -> (new RideCleansingExercise(source, sink)).execute();
    ExecutablePipeline<TaxiRide, TaxiRide> solution =
        (source, sink) -> (new RideCleansingSolution(source, sink)).execute();

    return new ComposedPipeline<>(exercise, solution);
  }
}

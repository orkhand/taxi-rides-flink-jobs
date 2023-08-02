/* (C)2023 */
package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.datatypes.TaxiRide;
import org.example.sources.TaxiRideGenerator;
import org.example.utils.GeoUtils;

/**
 * Solution to the Ride Cleansing exercise from the Flink training.
 *
 * <p>The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class RideCleansingSolution {

  private final SourceFunction<TaxiRide> source;
  private final SinkFunction<TaxiRide> sink;

  /** Creates a job using the source and sink provided. */
  public RideCleansingSolution(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink) {

    this.source = source;
    this.sink = sink;
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {
    RideCleansingSolution job =
        new RideCleansingSolution(new TaxiRideGenerator(), new PrintSinkFunction<>());

    job.execute();
  }

  /**
   * Creates and executes the long rides pipeline.
   *
   * @return {JobExecutionResult}
   * @throws Exception which occurs during job execution.
   */
  public JobExecutionResult execute() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // set up the pipeline
    env.addSource(source).filter(new NYCFilter()).addSink(sink);

    // run the pipeline and return the result
    return env.execute("Taxi Ride Cleansing");
  }

  /** Keep only those rides and both start and end in NYC. */
  public static class NYCFilter implements FilterFunction<TaxiRide> {
    @Override
    public boolean filter(TaxiRide taxiRide) {
      return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
          && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
    }
  }
}

/* (C)2023 */
package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.datatypes.TaxiRide;
import org.example.sources.TaxiRideGenerator;
import org.example.utils.GeoUtils;

/**
 * The task of this exercise is to split a data stream of taxi ride records. Rides that both start
 * and end within New York City should be printed to stdout, and any other rides should go to
 * stderr.
 */
public class RideSplitFilterSolution {

  private final SourceFunction<TaxiRide> source;
  private final SinkFunction<TaxiRide> sink;
  private final SinkFunction<TaxiRide> sidesink;

  /** Creates a job using the source and sinks provided. */
  public RideSplitFilterSolution(
      SourceFunction<TaxiRide> source,
      SinkFunction<TaxiRide> sink,
      SinkFunction<TaxiRide> sidesink) {

    this.source = source;
    this.sink = sink;
    this.sidesink = sidesink;
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {
    RideSplitFilterSolution job =
        new RideSplitFilterSolution(
            new TaxiRideGenerator(), new PrintSinkFunction<>(), new PrintSinkFunction<>(true));

    job.execute();
  }

  /**
   * Creates and executes the pipeline.
   *
   * @return {JobExecutionResult}
   * @throws Exception which occurs during job execution.
   */
  public JobExecutionResult execute() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // attach a stream of TaxiRides
    DataStream<TaxiRide> rides = env.addSource(source);

    // split the stream
    rides.filter(ride -> inNYC(ride)).addSink(sink).name("inside NYC");
    rides.filter(ride -> !inNYC(ride)).addSink(sidesink).name("outside NYC");

    // run the pipeline and return the result
    return env.execute("Split with filters");
  }

  /** Return true for rides that both start and end in NYC. */
  private static boolean inNYC(TaxiRide taxiRide) {
    return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
        && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
  }
}

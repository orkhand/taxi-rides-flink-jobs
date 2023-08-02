/* (C)2023 */
package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.example.datatypes.RideAndFare;
import org.example.datatypes.TaxiFare;
import org.example.datatypes.TaxiRide;
import org.example.sources.TaxiFareGenerator;
import org.example.sources.TaxiRideGenerator;
import org.example.utils.MissingSolutionException;

/**
 * The Stateful Enrichment exercise from the Flink training.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 */
public class RidesAndFaresExercise {

  private final SourceFunction<TaxiRide> rideSource;
  private final SourceFunction<TaxiFare> fareSource;
  private final SinkFunction<RideAndFare> sink;

  /** Creates a job using the sources and sink provided. */
  public RidesAndFaresExercise(
      SourceFunction<TaxiRide> rideSource,
      SourceFunction<TaxiFare> fareSource,
      SinkFunction<RideAndFare> sink) {

    this.rideSource = rideSource;
    this.fareSource = fareSource;
    this.sink = sink;
  }

  /**
   * Creates and executes the pipeline using the StreamExecutionEnvironment provided.
   *
   * @throws Exception which occurs during job execution.
   * @return {JobExecutionResult}
   */
  public JobExecutionResult execute() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // A stream of taxi ride START events, keyed by rideId.
    DataStream<TaxiRide> rides =
        env.addSource(rideSource).filter(ride -> ride.isStart).keyBy(ride -> ride.rideId);

    // A stream of taxi fare events, also keyed by rideId.
    DataStream<TaxiFare> fares = env.addSource(fareSource).keyBy(fare -> fare.rideId);

    // Create the pipeline.
    rides.connect(fares).flatMap(new EnrichmentFunction()).addSink(sink);

    // Execute the pipeline and return the result.
    return env.execute("Join Rides with Fares");
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {

    RidesAndFaresExercise job =
        new RidesAndFaresExercise(
            new TaxiRideGenerator(), new TaxiFareGenerator(), new PrintSinkFunction<>());

    job.execute();
  }

  public static class EnrichmentFunction
      extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> {

    @Override
    public void open(Configuration config) throws Exception {
      throw new MissingSolutionException();
    }

    @Override
    public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {
      throw new MissingSolutionException();
    }

    @Override
    public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {
      throw new MissingSolutionException();
    }
  }
}

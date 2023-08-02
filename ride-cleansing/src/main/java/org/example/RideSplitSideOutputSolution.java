/* (C)2023 */
package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.datatypes.TaxiRide;
import org.example.sources.TaxiRideGenerator;
import org.example.utils.GeoUtils;

/**
 * The task of this exercise is to split a data stream of taxi ride records. Rides that both start
 * and end within New York City should be printed to stdout, and any other rides should go to
 * stderr.
 */
public class RideSplitSideOutputSolution {

  private final SourceFunction<TaxiRide> source;
  private final SinkFunction<TaxiRide> sink;
  private final SinkFunction<TaxiRide> sidesink;

  private static final OutputTag<TaxiRide> outsideNYC = new OutputTag<TaxiRide>("outsideNYC") {};

  /** Creates a job using the source and sinks provided. */
  public RideSplitSideOutputSolution(
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
    RideSplitSideOutputSolution job =
        new RideSplitSideOutputSolution(
            new TaxiRideGenerator(), new PrintSinkFunction<>(), new PrintSinkFunction<>(true));

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

    // attach a stream of TaxiRides
    DataStream<TaxiRide> rides = env.addSource(source);

    // split the stream
    SingleOutputStreamOperator<TaxiRide> splitResult = rides.process(new StreamSplitter());

    splitResult.addSink(sink).name("inside NYC");
    splitResult.getSideOutput(outsideNYC).addSink(sidesink).name("outside NYC");

    // run the pipeline and return the result
    return env.execute("Split with side output");
  }

  public static class StreamSplitter extends ProcessFunction<TaxiRide, TaxiRide> {

    @Override
    public void processElement(TaxiRide taxiRide, Context ctx, Collector<TaxiRide> out)
        throws Exception {

      if (GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
          && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat)) {

        out.collect(taxiRide);
      } else {
        ctx.output(outsideNYC, taxiRide);
      }
    }
  }
}

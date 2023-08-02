/* (C)2023 */
package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.datatypes.TaxiFare;
import org.example.sources.TaxiFareGenerator;
import org.example.utils.MissingSolutionException;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

  private final SourceFunction<TaxiFare> source;
  private final SinkFunction<Tuple3<Long, Long, Float>> sink;

  /** Creates a job using the source and sink provided. */
  public HourlyTipsExercise(
      SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

    this.source = source;
    this.sink = sink;
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {

    HourlyTipsExercise job =
        new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

    job.execute();
  }

  /**
   * Create and execute the hourly tips pipeline.
   *
   * @return {JobExecutionResult}
   * @throws Exception which occurs during job execution.
   */
  public JobExecutionResult execute() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // start the data generator
    DataStream<TaxiFare> fares = env.addSource(source);

    // replace this with your solution
    if (true) {
      throw new MissingSolutionException();
    }

    // the results should be sent to the sink that was passed in
    // (otherwise the tests won't work)
    // you can end the pipeline with something like this:

    // DataStream<Tuple3<Long, Long, Float>> hourlyMax = ...
    // hourlyMax.addSink(sink);

    // execute the pipeline and return the result
    return env.execute("Hourly Tips");
  }
}

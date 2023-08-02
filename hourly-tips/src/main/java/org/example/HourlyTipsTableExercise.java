/* (C)2023 */
package org.example;

import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.datatypes.TaxiFare;
import org.example.sources.TaxiFareGenerator;
import org.example.utils.MissingSolutionException;

/**
 * The Hourly Tips exercise from the Flink training, using the Table/SQL API.
 *
 * <p>The goal of this exercise is to find the driver earning the most in tips in each hour.
 *
 * <ul>
 *   <li>Begin by removing or commenting out the code in this file that throws a
 *       MissingSolutionException.
 *   <li>Once you do, some of the tests in HourlyTipsTableTest will fail.
 *   <li>Find the problems in this implementation, and fix them.
 * </ul>
 */
public class HourlyTipsTableExercise {

  private final SourceFunction<TaxiFare> source;
  private final SinkFunction<Tuple3<Long, Long, Float>> sink;

  /** Creates a job using the source and sink provided. */
  public HourlyTipsTableExercise(
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

    HourlyTipsTableExercise job =
        new HourlyTipsTableExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

    job.execute();
  }

  /**
   * Create and execute the hourly tips pipeline.
   *
   * @return {JobExecutionResult}
   * @throws Exception which occurs during job execution.
   */
  public JobExecutionResult execute() throws Exception {

    // set up streaming execution environments
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // remove this, or comment it out
    if (true) {
      throw new MissingSolutionException();
    }

    // start the data generator
    DataStream<TaxiFare> fareStream = env.addSource(source);

    // convert the DataStream to a Table
    Schema fareSchema =
        Schema.newBuilder()
            .column("driverId", "BIGINT")
            .column("tip", "FLOAT")
            .column("startTime", "TIMESTAMP_LTZ(3)")
            .watermark("startTime", "startTime + INTERVAL '60' MINUTE")
            .build();
    tableEnv.createTemporaryView("fares", fareStream, fareSchema);

    // find the driver with the highest sum of tips for each hour
    Table hourlyMax =
        tableEnv.sqlQuery(
            "SELECT window_end, driverId, sumOfTips"
                + "  FROM ("
                + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end"
                + "        ORDER BY sumOfTips DESC) AS rownum"
                + "    FROM ("
                + "      SELECT window_start, window_end, driverId, SUM(tip) AS sumOfTips"
                + "      FROM TABLE("
                + "        TUMBLE(TABLE fares, DESCRIPTOR(startTime), INTERVAL '1' HOUR))"
                + "      GROUP BY window_start, window_end, driverId"
                + "    )"
                + "  ) WHERE rownum <= 2");

    // convert the query's results into a DataStream of the type expected by the tests
    DataStream<Tuple3<Long, Long, Float>> resultsAsStreamOfTuples =
        tableEnv
            .toDataStream(hourlyMax)
            .map(
                row ->
                    new Tuple3<>(
                        row.<LocalDateTime>getFieldAs("window_end")
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli(),
                        row.<Long>getFieldAs("driverId"),
                        row.<Float>getFieldAs("sumOfTips")))
            .returns(Types.TUPLE(Types.LONG, Types.LONG, Types.FLOAT));

    resultsAsStreamOfTuples.addSink(sink);

    // execute the pipeline
    return env.execute("Hourly Tips");
  }
}

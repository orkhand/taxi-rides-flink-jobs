/* (C)2023 */
package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.util.Collector;
import org.example.constants.Constants;
import org.example.datatypes.TaxiFare;
import org.example.serde.TaxiFareDeSerializationSchema;
import org.example.serde.Tuple3DeSerializer;
import org.example.sources.TaxiFareGenerator;

/**
 * Java reference implementation for the Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsSolution {
  private final SinkFunction<Tuple3<Long, Long, Float>> sink;

  /** Creates a job using the source and sink provided. */
  public HourlyTipsSolution(
      SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

    //  this.source = source;
    this.sink = sink;
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {

    HourlyTipsSolution job =
        new HourlyTipsSolution(new TaxiFareGenerator(), new PrintSinkFunction<>());

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

    KafkaSource<TaxiFare> kafkaSource =
        KafkaSource.<TaxiFare>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setTopics(Constants.TAXI_FARE_KAFKA_TOPIC)
            .setGroupId(Constants.TAXI_FARE_KAFKA_TOPIC + "-consumer-group")
            .setStartingOffsets(OffsetsInitializer.latest()) // earliest
            .setDeserializer(
                KafkaRecordDeserializationSchema.valueOnly(new TaxiFareDeSerializationSchema()))
            .build();

    DataStream<TaxiFare> fares =
        env.fromSource(
            kafkaSource,
            WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                .withTimestampAssigner((fare, t) -> fare.startTime.toEpochMilli()),
            Constants.TAXI_FARE_KAFKA_TOPIC + "-Kafka-Source");

    // compute tips per hour for each driver
    DataStream<Tuple3<Long, Long, Float>> hourlyTips =
        fares
            .keyBy((TaxiFare fare) -> fare.driverId)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new AddTips());

    // find the driver with the highest sum of tips for each hour
    DataStream<Tuple3<Long, Long, Float>> hourlyMax =
        hourlyTips.windowAll(TumblingEventTimeWindows.of(Time.minutes(1))).maxBy(2);

    /* You should explore how this alternative (commented out below) behaves.
     * In what ways is the same as, and different from, the solution above (using a windowAll)?
     */

    // DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips.keyBy(t -> t.f0).maxBy(2);

    hourlyMax.addSink(sink);

    KafkaSink<Tuple3<Long, Long, Float>> kafkaSink =
        KafkaSink.<Tuple3<Long, Long, Float>>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(Constants.HOURLY_TIPS_KAFKA_TOPIC)
                    .setValueSerializationSchema(new Tuple3DeSerializer())
                    .setPartitioner(new FlinkFixedPartitioner())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    hourlyMax.sinkTo(kafkaSink);

    // execute the transformation pipeline
    return env.execute("Hourly Tips");
  }

  /*
   * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
   */
  public static class AddTips
      extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

    @Override
    public void process(
        Long key,
        Context context,
        Iterable<TaxiFare> fares,
        Collector<Tuple3<Long, Long, Float>> out) {

      float sumOfTips = 0F;
      for (TaxiFare f : fares) {
        sumOfTips += f.tip;
      }
      out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
    }
  }
}

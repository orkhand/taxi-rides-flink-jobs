/* (C)2023 */
package org.example;

import java.time.Duration;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.util.Collector;
import org.example.constants.Constants;
import org.example.datatypes.RideAndFare;
import org.example.datatypes.TaxiFare;
import org.example.datatypes.TaxiRide;
import org.example.serde.RideAndFareSerializationSchema;
import org.example.serde.TaxiFareDeSerializationSchema;
import org.example.serde.TaxiRideDeSerializationSchema;
import org.example.sources.TaxiFareGenerator;
import org.example.sources.TaxiRideGenerator;

/**
 * Java reference implementation for the Stateful Enrichment exercise from the Flink training.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 */
public class RidesAndFaresSolution {

  private final SourceFunction<TaxiRide> rideSource;
  private final SourceFunction<TaxiFare> fareSource;
  private final SinkFunction<RideAndFare> sink;

  /** Creates a job using the sources and sink provided. */
  public RidesAndFaresSolution(
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
   * @param env The {StreamExecutionEnvironment}.
   * @return {JobExecutionResult}
   */
  /** Creates and executes the pipeline using the default StreamExecutionEnvironment. */
  public JobExecutionResult execute() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    return execute(env);
  }

  public JobExecutionResult execute(StreamExecutionEnvironment env) throws Exception {
    KafkaSource<TaxiRide> rideKafkaSource =
        KafkaSource.<TaxiRide>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setTopics(Constants.TAXI_RIDE_KAFKA_TOPIC)
            .setGroupId(Constants.TAXI_RIDE_KAFKA_TOPIC + "-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(
                KafkaRecordDeserializationSchema.valueOnly(new TaxiRideDeSerializationSchema()))
            .build();

    KafkaSource<TaxiFare> fareKafkaSource =
        KafkaSource.<TaxiFare>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setTopics(Constants.TAXI_FARE_KAFKA_TOPIC)
            .setGroupId(Constants.TAXI_FARE_KAFKA_TOPIC + "-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(
                KafkaRecordDeserializationSchema.valueOnly(new TaxiFareDeSerializationSchema()))
            .build();

    DataStream<TaxiRide> rides =
        env.fromSource(
                rideKafkaSource,
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                    .withTimestampAssigner(
                        (ride, streamRecordTimestamp) -> ride.eventTime.toEpochMilli()),
                Constants.TAXI_RIDE_KAFKA_TOPIC + "-Kafka-Source")
            .keyBy(ride -> ride.rideId);

    DataStream<TaxiFare> fares =
        env.fromSource(
                fareKafkaSource,
                WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                    .withTimestampAssigner(
                        (fare, streamRecordTimestamp) -> fare.startTime.toEpochMilli()),
                Constants.TAXI_FARE_KAFKA_TOPIC + "-Kafka-Source")
            .keyBy(fare -> fare.rideId);

    DataStream<RideAndFare> rideAndFareDataStream =
        rides.connect(fares).flatMap(new EnrichmentFunction()).uid("enrichment").name("enrichment");
    rideAndFareDataStream.addSink(sink);

    KafkaSink<RideAndFare> kafkaSink =
        KafkaSink.<RideAndFare>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(Constants.RIDE_AND_FARE_KAFKA_TOPIC)
                    .setValueSerializationSchema(new RideAndFareSerializationSchema())
                    .setPartitioner(new FlinkFixedPartitioner())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    rideAndFareDataStream.sinkTo(kafkaSink);
    rideAndFareDataStream.addSink(new PrintSinkFunction<>());

    return env.execute("Join Rides with Fares");
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {

    RidesAndFaresSolution job =
        new RidesAndFaresSolution(
            new TaxiRideGenerator(), new TaxiFareGenerator(), new PrintSinkFunction<>());

    // Setting up checkpointing so that the state can be explored with the State Processor API.
    // Generally it's better to separate configuration settings from the code,
    // but for this example it's convenient to have it here for running in the IDE.
    Configuration conf = new Configuration();
    conf.setString("state.backend", "filesystem");
    conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
    conf.setString("execution.checkpointing.interval", "10s");
    conf.setString(
        "execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

    job.execute(env);
  }

  public static class EnrichmentFunction
      extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> {

    private ValueState<TaxiRide> rideState;
    private ValueState<TaxiFare> fareState;

    @Override
    public void open(Configuration config) {

      rideState =
          getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
      fareState =
          getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
    }

    @Override
    public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {

      TaxiFare fare = fareState.value();
      if (fare != null) {
        fareState.clear();
        out.collect(new RideAndFare(ride, fare));
      } else {
        rideState.update(ride);
      }
    }

    @Override
    public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {

      TaxiRide ride = rideState.value();
      if (ride != null) {
        rideState.clear();
        out.collect(new RideAndFare(ride, fare));
      } else {
        fareState.update(fare);
      }
    }
  }
}

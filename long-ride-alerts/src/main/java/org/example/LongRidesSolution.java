/* (C)2023 */
package org.example;

import java.time.Duration;
import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.util.Collector;
import org.example.constants.Constants;
import org.example.datatypes.TaxiRide;
import org.example.serde.TaxiRideDeSerializationSchema;
import org.example.sources.TaxiRideGenerator;

/**
 * Java solution for the "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesSolution {

  private final SourceFunction<TaxiRide> source;
  private final SinkFunction<Long> sink;

  /** Creates a job using the source and sink provided. */
  public LongRidesSolution(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {

    this.source = source;
    this.sink = sink;
  }

  /**
   * Creates and executes the long rides pipeline.
   *
   * @return {JobExecutionResult}
   * @throws Exception which occurs during job execution.
   */
  public JobExecutionResult execute() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<TaxiRide> kafkaSource =
        KafkaSource.<TaxiRide>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setTopics(Constants.TAXI_RIDE_KAFKA_TOPIC)
            .setGroupId(Constants.TAXI_RIDE_KAFKA_TOPIC + "-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest()) // latest
            .setDeserializer(
                KafkaRecordDeserializationSchema.valueOnly(new TaxiRideDeSerializationSchema()))
            .build();

    DataStream<TaxiRide> rides =
        env.fromSource(
            kafkaSource,
            WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner(
                    (ride, streamRecordTimestamp) -> ride.eventTime.toEpochMilli()),
            Constants.TAXI_RIDE_KAFKA_TOPIC + "-Kafka-Source");

    // create the pipeline
    DataStream<Long> alertRideIdStream =
        rides.keyBy(ride -> ride.rideId).process(new AlertFunction());

    alertRideIdStream.addSink(sink);

    KafkaSink<Long> kafkaSink =
        KafkaSink.<Long>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(Constants.ALERT_RIDES_KAFKA_TOPIC)
                    .setValueSerializationSchema(
                        new TaxiRideDeSerializationSchema
                            .LongSerializationSchema()) // Change this to your serializer
                    .setPartitioner(new FlinkFixedPartitioner())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    alertRideIdStream.sinkTo(kafkaSink);
    alertRideIdStream.addSink(new PrintSinkFunction<>());

    // execute the transformation pipeline and return the result
    return env.execute("Alert Rides");
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {
    LongRidesSolution job =
        new LongRidesSolution(new TaxiRideGenerator(), new PrintSinkFunction<>());

    job.execute();
  }

  @VisibleForTesting
  public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

    private ValueState<TaxiRide> rideState;

    @Override
    public void open(Configuration config) {
      ValueStateDescriptor<TaxiRide> rideStateDescriptor =
          new ValueStateDescriptor<>("ride event", TaxiRide.class);
      rideState = getRuntimeContext().getState(rideStateDescriptor);
    }

    @Override
    public void processElement(TaxiRide ride, Context context, Collector<Long> out)
        throws Exception {

      TaxiRide firstRideEvent = rideState.value();

      if (firstRideEvent == null) {
        // whatever event comes first, remember it
        rideState.update(ride);

        if (ride.isStart) {
          // we will use this timer to check for rides that have gone on too long and may
          // not yet have an END event (or the END event could be missing)
          context.timerService().registerEventTimeTimer(getTimerTime(ride));
        }
      } else {
        if (ride.isStart) {
          if (rideTooLong(ride, firstRideEvent)) {
            out.collect(ride.rideId);
          }
        } else {
          // the first ride was a START event, so there is a timer unless it has fired
          context.timerService().deleteEventTimeTimer(getTimerTime(firstRideEvent));

          // perhaps the ride has gone on too long, but the timer didn't fire yet
          if (rideTooLong(firstRideEvent, ride)) {
            out.collect(ride.rideId);
          }
        }

        // both events have now been seen, we can clear the state
        // this solution can leak state if an event is missing
        // see DISCUSSION.md for more information
        rideState.clear();
      }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
        throws Exception {

      // the timer only fires if the ride was too long
      out.collect(rideState.value().rideId);

      // clearing now prevents duplicate alerts, but will leak state if the END arrives
      rideState.clear();
    }

    private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
      return Duration.between(startEvent.eventTime, endEvent.eventTime)
              .compareTo(Duration.ofHours(2))
          > 0;
    }

    private long getTimerTime(TaxiRide ride) throws RuntimeException {
      if (ride.isStart) {
        return ride.eventTime.plusSeconds(120 * 60).toEpochMilli();
      } else {
        throw new RuntimeException("Can not get start time from END event.");
      }
    }
  }
}

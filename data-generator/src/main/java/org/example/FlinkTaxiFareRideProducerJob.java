/* (C)2023 */
package org.example;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.example.constants.Constants;
import org.example.datatypes.TaxiFare;
import org.example.datatypes.TaxiRide;
import org.example.serde.TaxiFareDeSerializationSchema;
import org.example.serde.TaxiRideDeSerializationSchema;
import org.example.sources.TaxiFareGenerator;
import org.example.sources.TaxiRideGenerator;

public class FlinkTaxiFareRideProducerJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<TaxiRide> taxiRideDataStream = env.addSource(new TaxiRideGenerator());
    DataStream<TaxiFare> taxiFareDataStream = env.addSource(new TaxiFareGenerator());

    KafkaSink<TaxiRide> rideSink =
        KafkaSink.<TaxiRide>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(Constants.TAXI_RIDE_KAFKA_TOPIC)
                    .setValueSerializationSchema(new TaxiRideDeSerializationSchema())
                    .setPartitioner(new FlinkFixedPartitioner())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    KafkaSink<TaxiFare> fareSink =
        KafkaSink.<TaxiFare>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(Constants.TAXI_FARE_KAFKA_TOPIC)
                    .setValueSerializationSchema(new TaxiFareDeSerializationSchema())
                    .setPartitioner(new FlinkFixedPartitioner())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    taxiRideDataStream.sinkTo(rideSink);
    taxiRideDataStream.addSink(new PrintSinkFunction<>());

    taxiFareDataStream.sinkTo(fareSink);
    taxiFareDataStream.addSink(new PrintSinkFunction<>());

    env.execute("Combined Taxi Producer Job");
  }
}

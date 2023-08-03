/* (C)2023 */
package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.datatypes.TaxiFare;
import org.example.serde.TaxiFareDeSerializationSchema;

public class FlinkTaxiFareConsumerJob {
  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a Kafka consumer source
    KafkaSource<TaxiFare> source =
        KafkaSource.<TaxiFare>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setTopics(Constants.TAXI_FARE_KAFKA_TOPIC)
            .setGroupId(Constants.TAXI_FARE_KAFKA_TOPIC + "-consumer-group")
            .setStartingOffsets(OffsetsInitializer.latest()) // earliest
            .setDeserializer(
                KafkaRecordDeserializationSchema.valueOnly(new TaxiFareDeSerializationSchema()))
            .build();

    DataStream<TaxiFare> kafkaStream =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            Constants.TAXI_FARE_KAFKA_TOPIC + "-Kafka-Source");

    // Apply a simple transformation: printing the messages
    kafkaStream.print();

    // Execute the Flink job
    env.execute(Constants.TAXI_FARE_KAFKA_TOPIC + " Kafka Consumer Job");
  }
}

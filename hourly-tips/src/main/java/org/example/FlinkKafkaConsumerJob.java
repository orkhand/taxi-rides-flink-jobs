/* (C)2023 */
package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class FlinkKafkaConsumerJob {
  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a Kafka consumer source
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setTopics("test")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    DataStream<String> kafkaStream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    // Apply a simple transformation: printing the messages
    kafkaStream.print();

    // Execute the Flink job
    env.execute("Kafka Consumer Job");
  }
}

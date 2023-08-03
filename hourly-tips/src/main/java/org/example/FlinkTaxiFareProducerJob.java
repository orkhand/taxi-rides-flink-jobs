/* (C)2023 */
package org.example;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.example.datatypes.TaxiFare;
import org.example.serde.TaxiFareDeSerializationSchema;
import org.example.sources.TaxiFareGenerator;

public class FlinkTaxiFareProducerJob {
  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<TaxiFare> taxiFareDataStream = env.addSource(new TaxiFareGenerator());

    KafkaSink<TaxiFare> sink =
        KafkaSink.<TaxiFare>builder()
            .setBootstrapServers(Constants.KAFKA_BROKER_ENDPOINT)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(Constants.TAXI_FARE_KAFKA_TOPIC)
                    .setValueSerializationSchema(new TaxiFareDeSerializationSchema())
                    .setPartitioner(new FlinkFixedPartitioner())
                    // .setTopicSelector((element) -> {<your-topic-selection-logic>})
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    taxiFareDataStream.sinkTo(sink);
    // Execute the Flink job
    env.execute(Constants.TAXI_FARE_KAFKA_TOPIC + " Kafka Producer Job");
  }
}

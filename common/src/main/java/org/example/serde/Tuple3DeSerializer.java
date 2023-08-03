/* (C)2023 */
package org.example.serde;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Tuple3DeSerializer
    implements DeserializationSchema<Tuple3<Long, Long, Float>>,
        SerializationSchema<Tuple3<Long, Long, Float>> {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Tuple3<Long, Long, Float> deserialize(byte[] message) throws IOException {
    // Deserialize byte array back to Tuple3<Long, Long, Float>
    // Modify this code based on your preferred serialization library (e.g., Jackson, Gson)
    return deserializeFromJson(new String(message));
  }

  private Tuple3<Long, Long, Float> deserializeFromJson(String json) throws IOException {
    // Implement JSON deserialization here based on your preferred library
    // For example, using Jackson ObjectMapper
    return objectMapper.readValue(json, new TypeReference<Tuple3<Long, Long, Float>>() {});
  }

  @Override
  public boolean isEndOfStream(Tuple3<Long, Long, Float> nextElement) {
    return false;
  }

  @Override
  public TypeInformation<Tuple3<Long, Long, Float>> getProducedType() {
    return TypeInformation.of(new TypeHint<Tuple3<Long, Long, Float>>() {});
  }

  @Override
  public byte[] serialize(Tuple3<Long, Long, Float> tuple) {
    // Serialize Tuple3<Long, Long, Float> into a byte array or a specific format
    // For example, you can use JSON serialization here
    // Modify this code based on your preferred serialization library (e.g., Jackson, Gson)
    try {
      return serializeToJson(tuple).getBytes();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private String serializeToJson(Tuple3<Long, Long, Float> tuple) throws JsonProcessingException {
    // Implement JSON serialization here based on your preferred library
    // For example, using Jackson ObjectMapper
    return objectMapper.writeValueAsString(tuple);
  }
}

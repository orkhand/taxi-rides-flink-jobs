/* (C)2023 */
package org.example.serde;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.example.datatypes.TaxiFare;

public class TaxiFareDeSerializationSchema
    implements DeserializationSchema<TaxiFare>, SerializationSchema<TaxiFare> {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.registerModule(new JavaTimeModule());
  }

  private static final long serialVersionUID = 1L;

  @Override
  public byte[] serialize(TaxiFare taxiFare) {
    // Serialize TaxiFare object into a byte array or a specific format
    // For example, you can use JSON serialization here
    // Modify this code based on your preferred serialization library (e.g., Jackson, Gson)
    try {
      return serializeToJson(taxiFare).getBytes();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private String serializeToJson(TaxiFare taxiFare) throws JsonProcessingException {
    // Implement JSON serialization here based on your preferred library
    // For example, using Jackson ObjectMapper
    return objectMapper.writeValueAsString(taxiFare);
  }

  @Override
  public TaxiFare deserialize(byte[] message) throws IOException {
    // Deserialize byte array back to TaxiFare object
    // Modify this code based on your preferred serialization library (e.g., Jackson, Gson)
    return deserializeFromJson(new String(message));
  }

  private TaxiFare deserializeFromJson(String json) throws IOException {
    // Implement JSON deserialization here based on your preferred library
    // For example, using Jackson ObjectMapper
    return objectMapper.readValue(json, TaxiFare.class);
  }

  @Override
  public boolean isEndOfStream(TaxiFare nextElement) {
    return false;
  }

  @Override
  public TypeInformation<TaxiFare> getProducedType() {
    return TypeInformation.of(TaxiFare.class);
  }
}

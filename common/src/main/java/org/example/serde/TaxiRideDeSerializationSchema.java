/* (C)2023 */
package org.example.serde;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.example.datatypes.TaxiRide;

public class TaxiRideDeSerializationSchema
    implements DeserializationSchema<TaxiRide>, SerializationSchema<TaxiRide> {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.registerModule(new JavaTimeModule());
  }

  private static final long serialVersionUID = 1L;

  @Override
  public byte[] serialize(TaxiRide taxiRide) {
    try {
      return objectMapper.writeValueAsBytes(taxiRide);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing TaxiRide", e);
    }
  }

  public TaxiRide deserialize(byte[] bytes) throws IOException {
    return objectMapper.readValue(bytes, TaxiRide.class);
  }

  @Override
  public boolean isEndOfStream(TaxiRide taxiRide) {
    return false;
  }

  @Override
  public TypeInformation<TaxiRide> getProducedType() {
    return TypeInformation.of(TaxiRide.class);
  }

  // Add the LongSerializationSchema class here
  public static class LongSerializationSchema
      implements SerializationSchema<Long>, DeserializationSchema<Long> {
    @Override
    public byte[] serialize(Long value) {
      return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Long deserialize(byte[] message) throws IOException {
      String strValue = new String(message, StandardCharsets.UTF_8);
      return Long.parseLong(strValue);
    }

    @Override
    public boolean isEndOfStream(Long nextElement) {
      return false;
    }

    @Override
    public TypeInformation<Long> getProducedType() {
      return BasicTypeInfo.LONG_TYPE_INFO;
    }
  }
}

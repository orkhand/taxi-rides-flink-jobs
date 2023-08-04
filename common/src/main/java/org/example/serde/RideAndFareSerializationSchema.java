/* (C)2023 */
package org.example.serde;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.example.datatypes.RideAndFare;

public class RideAndFareSerializationSchema
    implements DeserializationSchema<RideAndFare>, SerializationSchema<RideAndFare> {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.registerModule(new JavaTimeModule());
  }

  private static final long serialVersionUID = 1L;

  @Override
  public byte[] serialize(RideAndFare rideAndFare) {
    try {
      return objectMapper.writeValueAsBytes(rideAndFare);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing RideAndFare", e);
    }
  }

  @Override
  public RideAndFare deserialize(byte[] bytes) throws IOException {
    return objectMapper.readValue(bytes, RideAndFare.class);
  }

  @Override
  public boolean isEndOfStream(RideAndFare nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RideAndFare> getProducedType() {
    return TypeInformation.of(RideAndFare.class);
  }
}

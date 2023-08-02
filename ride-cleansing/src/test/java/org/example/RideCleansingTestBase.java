/* (C)2023 */
package org.example;

import java.time.Instant;
import org.example.datatypes.TaxiRide;

public class RideCleansingTestBase {

  public static TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
    return new TaxiRide(
        1L, true, Instant.EPOCH, startLon, startLat, endLon, endLat, (short) 1, 0, 0);
  }
}

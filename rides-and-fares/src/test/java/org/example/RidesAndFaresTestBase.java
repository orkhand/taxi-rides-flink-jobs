/* (C)2023 */
package org.example;

import java.time.Instant;
import org.example.datatypes.TaxiFare;
import org.example.datatypes.TaxiRide;

public class RidesAndFaresTestBase {

  public static TaxiRide testRide(long rideId) {
    return new TaxiRide(rideId, true, Instant.EPOCH, 0F, 0F, 0F, 0F, (short) 1, 0, rideId);
  }

  public static TaxiFare testFare(long rideId) {
    return new TaxiFare(rideId, 0, rideId, Instant.EPOCH, "", 0F, 0F, 0F);
  }
}

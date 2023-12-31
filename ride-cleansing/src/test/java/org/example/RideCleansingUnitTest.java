/* (C)2023 */
package org.example;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.example.datatypes.TaxiRide;
import org.junit.Test;
import testing.ComposedFilterFunction;

public class RideCleansingUnitTest extends RideCleansingTestBase {

  public ComposedFilterFunction<TaxiRide> filterFunction() {
    return new ComposedFilterFunction<>(
        new RideCleansingExercise.NYCFilter(), new RideCleansingSolution.NYCFilter());
  }

  @Test
  public void testRideThatStartsAndEndsInNYC() throws Exception {

    TaxiRide atPennStation = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
    assertThat(filterFunction().filter(atPennStation)).isTrue();
  }

  @Test
  public void testRideThatStartsOutsideNYC() throws Exception {

    TaxiRide fromThePole = testRide(0, 90, -73.9947F, 40.750626F);
    assertThat(filterFunction().filter(fromThePole)).isFalse();
  }

  @Test
  public void testRideThatEndsOutsideNYC() throws Exception {

    TaxiRide toThePole = testRide(-73.9947F, 40.750626F, 0, 90);
    assertThat(filterFunction().filter(toThePole)).isFalse();
  }

  @Test
  public void testRideThatStartsAndEndsOutsideNYC() throws Exception {

    TaxiRide atNorthPole = testRide(0, 90, 0, 90);
    assertThat(filterFunction().filter(atNorthPole)).isFalse();
  }
}

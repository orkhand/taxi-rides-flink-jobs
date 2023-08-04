/* (C)2023 */
package org.example.constants;

public class Constants {
  /** This is IP address of Network.Services.cp-helm-charts-<any-number></>-cp-kafka */
  public static final String KAFKA_BROKER_ENDPOINT = "172.20.100.205:9092";

  public static final String TAXI_FARE_KAFKA_TOPIC = "taxi-fare";

  public static final String HOURLY_TIPS_KAFKA_TOPIC = "hourly-tips";

  public static final String TAXI_RIDE_KAFKA_TOPIC = "taxi-ride";

  public static final String ALERT_RIDES_KAFKA_TOPIC = "long-ride-alerts";
  public static final String RIDE_AND_FARE_KAFKA_TOPIC = "rides-and-fares";
}

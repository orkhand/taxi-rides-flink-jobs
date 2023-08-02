ARG FLINK_VERSION=1.17.1
FROM adoptopenjdk/maven-openjdk11 as builder
COPY . src

RUN cd src \
    && mvn spotless:apply install package -DskipTests


ARG FLINK_VERSION
FROM flink:${FLINK_VERSION}

ENV FLINK_HOME=/opt/flink

COPY --from=builder /src/hourly-tips/target/hourly-tips-1.0-SNAPSHOT.jar $FLINK_HOME/lib
COPY --from=builder /src/long-ride-alerts/target/long-ride-alerts-1.0-SNAPSHOT.jar $FLINK_HOME/lib
COPY --from=builder /src/ride-cleansing/target/ride-cleansing-1.0-SNAPSHOT.jar $FLINK_HOME/lib
COPY --from=builder /src/rides-and-fares/target/rides-and-fares-1.0-SNAPSHOT.jar $FLINK_HOME/lib

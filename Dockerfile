FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q package -DskipTests

FROM flink:1.17.1-scala_2.12-java17
WORKDIR /opt/flink
COPY --from=build /build/target/flink-jms-table-connector-*.jar /opt/flink/usrlib/
COPY sql-script.sql /opt/flink/sql-script.sql
CMD ["bash", "-c", "./bin/start-cluster.sh && ./bin/sql-client.sh -f /opt/flink/sql-script.sql"]

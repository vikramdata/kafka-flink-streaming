FROM flink:1.20.5-scala_2.12-java17

# Connectors are not bundled with Flink: add Kafka, JDBC and the Postgres driver.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
ADD --chmod=644 ${MAVEN}/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar /opt/flink/lib/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/flink/lib/

COPY sql/ /opt/sql/
COPY submit-job.sh /opt/submit-job.sh

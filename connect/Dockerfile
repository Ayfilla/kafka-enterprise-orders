FROM confluentinc/cp-kafka-connect:7.6.1

# -------------------------------------------
# 1. Install unzip (mandatory for Couchbase connector)
# -------------------------------------------
USER root
RUN microdnf install -y unzip && microdnf clean all

# -------------------------------------------
# 2. Install JDBC connector
# -------------------------------------------
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest

# -------------------------------------------
# 3. Copy Couchbase connector (you already downloaded it)
# -------------------------------------------
COPY couchbase-kafka-connector.zip /tmp/couchbase.zip

# -------------------------------------------
# 4. Unzip Couchbase connector manually (works on M1/M2 Mac)
# -------------------------------------------
RUN mkdir -p /usr/share/java/couchbase && \
    unzip /tmp/couchbase.zip -d /usr/share/java/couchbase && \
    rm /tmp/couchbase.zip

# -------------------------------------------
# 5. Copy our custom connect-distributed.properties
# (this forces Kafka Connect to use kafka:9092 instead of localhost:9092)
# -------------------------------------------
COPY connect-distributed.properties /etc/kafka/connect-distributed.properties

# -------------------------------------------
# 6. Plugin path (Couchbase + JDBC)
# -------------------------------------------
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components,/usr/share/java/couchbase"

# -------------------------------------------
# 7. Run Kafka Connect with our configuration
# -------------------------------------------
USER appuser
CMD ["connect-distributed", "/etc/kafka/connect-distributed.properties"]


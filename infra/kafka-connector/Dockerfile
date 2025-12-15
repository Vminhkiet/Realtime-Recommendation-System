FROM confluentinc/cp-kafka-connect:7.5.0

USER root

# Cài các connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0 && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.0

# Cài PostgreSQL JDBC driver
RUN POSTGRES_LIB=/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib && \
    mkdir -p $POSTGRES_LIB && \
    curl -fL -o $POSTGRES_LIB/postgresql-42.6.0.jar \
        https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

USER appuser

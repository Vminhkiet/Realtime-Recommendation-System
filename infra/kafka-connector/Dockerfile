FROM confluentinc/cp-kafka-connect:7.3.0

USER root

# Cài đặt curl và unzip để tải connector
RUN yum install -y curl unzip

# Cài đặt Confluent Hub Client để tải plugins
RUN curl -L -o /tmp/confluent-hub-client-latest.tar.gz http://client.hub.confluent.io/confluent-hub-client-latest.tar.gz && \
    tar -zxvf /tmp/confluent-hub-client-latest.tar.gz -C /tmp/ && \
    # Cài đặt JDBC Connector (để nối với Postgres/Mongo nếu cần sau này)
    /tmp/bin/confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.0 && \
    # Cài đặt S3 Connector (nếu cần nối MinIO)
    /tmp/bin/confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.4.0

USER appuser
# Build base image cho Spark & dashboard
docker build -t spark-base ./base

# Build Kafka Connect image
docker build -t kafka-connect ./infra/kafka-connector
docker-compose up -d
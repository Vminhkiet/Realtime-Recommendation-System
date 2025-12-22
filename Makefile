# ==============================================================================
# 🛠️ CẤU HÌNH BIẾN MÔI TRƯỜNG (CONFIGURATION)
# ==============================================================================

# Sử dụng 'docker compose' (V2) thay vì 'docker-compose' (V1 cũ) để tránh lỗi
DOCKER_COMPOSE = docker-compose
SPARK_MASTER   = spark-master
CONNECT_HOST   = localhost
CONNECT_PORT   = 8083
PYTHON = .venv/bin/python
PIP = .venv/bin/pip
PREPARE_SCRIPT = src/ai_core/prepare_model.py
# Định nghĩa màu sắc để log dễ nhìn hơn
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
RESET  := $(shell tput -Txterm sgr0)

.PHONY: help up down restart build logs ps setup process train stream sim inspect test-ai clean

# ==============================================================================
# 🚀 1. VẬN HÀNH HẠ TẦNG (INFRASTRUCTURE OPERATIONS)
# ==============================================================================

## Hiển thị danh sách lệnh
help:
	@echo ''
	@echo '${YELLOW}Usage:${RESET} make ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${GREEN}%-20s${RESET} %s\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

## Bật toàn bộ hệ thống (Background mode)
up:
	@echo "${YELLOW}Starting infrastructure...${RESET}"
	$(DOCKER_COMPOSE) up -d

## Tắt toàn bộ hệ thống
down:
	@echo "${YELLOW}Stopping infrastructure...${RESET}"
	$(DOCKER_COMPOSE) -f src/serving/docker-compose.yml down
	$(DOCKER_COMPOSE) down

## Khởi động lại hệ thống
restart: down up

## Build lại images (Chạy khi sửa Dockerfile hoặc requirements.txt)
build:
	docker build -t spark-base ./base
	docker build -t kafka-connect ./infra/kafka-connector
	docker build -t inference-service ./base/serving
	$(DOCKER_COMPOSE) up -d

## Xem danh sách container đang chạy
ps:
	docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}"

## Xem logs (Ví dụ: make logs s=spark-master)
logs:
	$(DOCKER_COMPOSE) logs -f $(s)

# ==============================================================================
# 🧠 2. QUY TRÌNH HUẤN LUYỆN AI (AI PIPELINE)
# ==============================================================================


## B2. Xử lý dữ liệu (Raw JSON -> Dataset.pkl)
process_beauty:
	@echo "${YELLOW}Running Data Processing...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/processing/streaming/spark_process_beauty.py
process_game:
	@echo "${YELLOW}Running Data Processing...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/processing/streaming/spark_process_game.py
## B3. Huấn luyện Model (Dataset.pkl -> Model.keras)
train:
	@echo "${YELLOW}Running Model Training...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/train.py

train_game:
	@echo "${YELLOW}Running Model Training...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/train_game.py

## B4. Test thử Model sau khi train
test-ai:
	@echo "${YELLOW}Testing Trained Model...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/test_model.py

eval-ai:
	@echo "📊 Running Full Evaluation..."
	# Cài tqdm cho đẹp (nếu chưa có), sau đó chạy evaluate
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/evaluate.py

eval-game-ai:
	@echo "📊 Running Full Evaluation..."
	# Cài tqdm cho đẹp (nếu chưa có), sau đó chạy evaluate
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/evaluate_game.py

eval-metric:
	@echo "📊 Running Full Evaluation..."
	# Cài tqdm cho đẹp (nếu chưa có), sau đó chạy evaluate
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/evaluate_metrics.py


prepare-model:
	@echo "${YELLOW}Converting Keras model to SavedModel format...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 $(PREPARE_SCRIPT)
	@echo "${YELLOW}Converting Keras model to SavedModel format...${RESET}"
	@echo "${GREEN}Model prepared successfully in data/model_registry/1/${RESET}"

# Lệnh tổng hợp: Chuyển đổi model và khởi động lại TF Serving
reload-tf: prepare-model
	@echo "${YELLOW}Restarting TF Serving to load new model version...${RESET}"
	docker-compose restart tf-serving
	@echo "${GREEN}TF Serving is reloading. Check logs with: docker logs -f tf-serving${RESET}"
# ==============================================================================
# ⚙️ 3. SETUP DỮ LIỆU & KẾT NỐI (DATA SETUP - RUN ONCE)
# ==============================================================================

# Setup toàn bộ (Metadata -> TimescaleDB -> Connectors)
setup:
	@echo "${YELLOW}--- 1. Importing Metadata to MongoDB ---${RESET}"
	# Nạp thông tin sản phẩm (Tên, Giá, Ảnh) vào MongoDB
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/utils/init_mongo.py

# 	@echo "\n${YELLOW}--- 2. Creating TimescaleDB Hypertable ---${RESET}"
# 	# Tạo bảng lưu log hành vi người dùng trong TimescaleDB
# 	docker exec -i timescaledb psql -U postgres -d ecommerce_logs -c "\
# 		CREATE TABLE IF NOT EXISTS user_activity ( \
# 			time TIMESTAMPTZ NOT NULL, \
# 			user_id TEXT, \
# 			item_id TEXT, \
# 			action_type TEXT, \
# 			device TEXT \
# 		); \
# 		SELECT create_hypertable('user_activity', 'time', if_not_exists => TRUE);" || true

# 	@echo "\n${YELLOW}--- 3. Registering Kafka Connectors ---${RESET}"
# 	# Đăng ký Connector (JSON viết 1 dòng để tránh lỗi Makefile)
# 	@echo "Waiting for Kafka Connect to be ready..."
# 	@sleep 5
# 	@curl -s -X POST http://$(CONNECT_HOST):$(CONNECT_PORT)/connectors \
# 		-H "Content-Type: application/json" \
# 		-d '{"name": "timescale-sink", "config": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "tasks.max": "1", "topics": "user_clicks", "connection.url": "jdbc:postgresql://timescaledb:5432/ecommerce_logs", "connection.user": "postgres", "connection.password": "password", "auto.create": "true", "insert.mode": "insert"}}' || echo "Connector might already exist."
# 	@echo "\n${GREEN}Setup Completed!${RESET}"

init-timescale:
	sudo rm -rf ./data/timescale_data

setup-timescale-sink:
	@echo "♻️  Gỡ bỏ Connector cũ để reset bộ nhớ đệm..."
	@curl -s -X DELETE http://localhost:8083/connectors/sink-timescale-interactions || true
	@sleep 2
	@echo "🚀 Đang thiết lập JDBC Sink Connector với auto.evolve=false..."
	@curl -s -X POST -H "Content-Type: application/json" \
		--data @connectors/sink_timescale.json \
		http://localhost:8083/connectors
	@echo "\n✅ Đã gửi yêu cầu. Đợi 5s để Task khởi động..."
	@sleep 5
	@curl -s http://localhost:8083/connectors/sink-timescale-interactions/status | jq

setup-minio-sink:
	@echo "♻️  Đang gỡ bỏ Connector cũ..."
	# 1. Xóa Connector cũ (nếu có)
	@curl -s -X DELETE http://localhost:8083/connectors/sink-minio-processed-parquet || true
	
	@echo "⏳ Đợi 3 giây..."
	@sleep 3
	
	@echo "🚀 Đang deploy Connector từ file: connectors/sink_minio.json"
	# 2. Tạo mới với đúng đường dẫn file bạn yêu cầu
	@curl -s -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @connectors/sink_minio_fake.json
	
	@echo "\n✅ Setup Completed! Kiểm tra trạng thái:"
	@sleep 1
	@curl -s http://localhost:8083/connectors/sink-minio-processed-parquet/status | jq

setup-mongo:
	@echo "${YELLOW}--- 1. Importing Metadata to MongoDB ---${RESET}"
	# Nạp thông tin sản phẩm (Tên, Giá, Ảnh) vào MongoDB
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/utils/init_mongo.py
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/utils/init_mongo_meta.py

# 	@echo "\n${YELLOW}--- 2. Creating TimescaleDB Hypertable ---${RESET}"
# 	# Tạo bảng lưu log hành vi người dùng trong TimescaleDB
# 	docker exec -i timescaledb psql -U postgres -d ecommerce_logs -c "\
# 		CREATE TABLE IF NOT EXISTS user_activity ( \
# 			time TIMESTAMPTZ NOT NULL, \
# 			user_id TEXT, \
# 			item_id TEXT, \
# 			action_type TEXT, \
# 			device TEXT \
# 		); \
# 		SELECT create_hypertable('user_activity', 'time', if_not_exists => TRUE);" || true

# 	@echo "\n${YELLOW}--- 3. Registering Kafka Connectors (AVRO MODE) ---${RESET}"
# 	# Xóa connector cũ nếu có để tránh xung đột
# 	@curl -s -X DELETE http://$(CONNECT_HOST):$(CONNECT_PORT)/connectors/timescale-sink || true
# 	@curl -s -X DELETE http://$(CONNECT_HOST):$(CONNECT_PORT)/connectors/timescale-sink-avro || true
	
# 	@echo "Waiting for Kafka Connect to be ready..."
# 	@sleep 5
# 	# [QUAN TRỌNG] JSON bên dưới đã được viết thành 1 dòng để tránh lỗi Makefile
# 	@curl -s -X POST http://$(CONNECT_HOST):$(CONNECT_PORT)/connectors \
# 		-H "Content-Type: application/json" \
# 		-d '{"name": "timescale-sink-avro", "config": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "tasks.max": "1", "topics": "user_clicks", "connection.url": "jdbc:postgresql://timescaledb:5432/ecommerce_logs", "connection.user": "postgres", "connection.password": "password", "auto.create": "true", "insert.mode": "insert", "key.converter": "org.apache.kafka.connect.storage.StringConverter", "value.converter": "io.confluent.connect.avro.AvroConverter", "value.converter.schema.registry.url": "http://schema-registry:8081"}}' || echo "Connector setup failed."
# 	@echo "\n${GREEN}Setup Completed!${RESET}"

# ==============================================================================
# 🌊 4. CHẠY DEMO REAL-TIME (RUNTIME)
# ==============================================================================

## Terminal 1: Chạy Simulator (Giả lập người dùng click)
sim:
	@echo "${YELLOW}Running Simulation inside Docker...${RESET}"
	# Chạy producer với biến môi trường Kafka nội bộ
# 	docker exec -it -w /home/spark/work -e KAFKA_SERVER=kafka:29092 $(SPARK_MASTER) python3 src/simulation/main_producer.py
	docker exec -it spark-master pip install confluent-kafka fastavro requests Faker authlib
	docker exec -it -w /home/spark/work \
		-e KAFKA_BOOTSTRAP=kafka:29092 \
		-e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
		spark-master python3 src/simulation/avro_producer.py

## Terminal 2: Chạy Spark Streaming (AI Inference Real-time)
stream:
	@echo "${YELLOW}Submitting Spark Streaming Job...${RESET}"
	docker exec -it -e PYTHONPATH=/home/spark/work -w /home/spark/work spark-master spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1 \
		/home/spark/work/src/serving/run_inference.py

streaming:
	docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/home/spark/work/src/processing/streaming/feature_engineering.py
ETL:
	docker exec -it -w /home/spark/work spark-master bash -c "\
		pip install boto3 && \
		spark-submit \
		--packages org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		src/processing/batch_etl_minio.py"

UPLOAD:
	docker exec -it -w /home/spark/work spark-master bash -c "\
		pip install boto3 && \
		spark-submit \
		--packages org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		src/utils/upload_local_to_minio.py"

ETL-TRAIN:
	docker exec -it -w /home/spark/work spark-master bash -c "\
		pip install boto3 && \
		spark-submit \
		--packages org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		src/processing/batch/batch_etl_train.py"

auto-train:
	@echo "${YELLOW}🚀 Starting Automated Training Pipeline...${RESET}"
	# Đảm bảo cài đặt các thư viện cần thiết trong container
	#docker exec -u 0 spark-master pip install tensorflow keras-hub s3fs pymongo pandas
	# Thực thi script huấn luyện lấy dữ liệu từ Data Lake (MinIO)
	docker exec -it -w /home/spark/work spark-master spark-submit \
        --packages org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        src/utils/debug_view.py
	@echo "${GREEN}✅ Training completed. TF Serving will hot-reload the new model version.${RESET}"

up-serving:
	@echo "${YELLOW}Starting Inference Service...${RESET}"
	docker-compose -f src/serving/docker-compose.yml up -d

down-serving:
	@echo "${RED}Stopping Inference Service...${RESET}"
	docker-compose -f src/serving/docker-compose.yml down
## Dọn dẹp dữ liệu rác (CẨN THẬN: Xóa sạch Database)
clean-data: down
	@echo "${YELLOW}Cleaning all data volumes...${RESET}"
	sudo rm -rf data/mongo_data/* data/timescale_data/* data/redis_data/*
	@echo "${GREEN}All data cleaned!${RESET}"
# ==============================================================================
# 🛠️ CẤU HÌNH BIẾN MÔI TRƯỜNG (CONFIGURATION)
# ==============================================================================

# Sử dụng 'docker compose' (V2) thay vì 'docker-compose' (V1 cũ) để tránh lỗi
DOCKER_COMPOSE = docker-compose
SPARK_MASTER   = spark-master
CONNECT_HOST   = localhost
CONNECT_PORT   = 8083

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
	$(DOCKER_COMPOSE) down

## Khởi động lại hệ thống
restart: down up

## Build lại images (Chạy khi sửa Dockerfile hoặc requirements.txt)
build:
	docker build -t spark-base ./base
	docker build -t kafka-connect ./infra/kafka-connector
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

## B1. Kiểm tra dữ liệu thô (Inspect Raw Data)
inspect:
	@echo "${YELLOW}Inspecting Raw Data...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/inspect_data.py

## B2. Xử lý dữ liệu (Raw JSON -> Dataset.pkl)
process:
	@echo "${YELLOW}Running Data Processing...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/data_process.py

## B3. Huấn luyện Model (Dataset.pkl -> Model.keras)
train:
	@echo "${YELLOW}Running Model Training...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/train.py

## B4. Test thử Model sau khi train
test-ai:
	@echo "${YELLOW}Testing Trained Model...${RESET}"
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/ai_core/test_model.py

# ==============================================================================
# ⚙️ 3. SETUP DỮ LIỆU & KẾT NỐI (DATA SETUP - RUN ONCE)
# ==============================================================================

## Setup toàn bộ (Metadata -> TimescaleDB -> Connectors)
setup:
	@echo "${YELLOW}--- 1. Importing Metadata to MongoDB ---${RESET}"
	# Nạp thông tin sản phẩm (Tên, Giá, Ảnh) vào MongoDB
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/utils/init_mongo.py

	@echo "\n${YELLOW}--- 2. Creating TimescaleDB Hypertable ---${RESET}"
	# Tạo bảng lưu log hành vi người dùng trong TimescaleDB
	docker exec -i timescaledb psql -U postgres -d ecommerce_logs -c "\
		CREATE TABLE IF NOT EXISTS user_activity ( \
			time TIMESTAMPTZ NOT NULL, \
			user_id TEXT, \
			item_id TEXT, \
			action_type TEXT, \
			device TEXT \
		); \
		SELECT create_hypertable('user_activity', 'time', if_not_exists => TRUE);" || true

	@echo "\n${YELLOW}--- 3. Registering Kafka Connectors ---${RESET}"
	# Đăng ký Connector (JSON viết 1 dòng để tránh lỗi Makefile)
	@echo "Waiting for Kafka Connect to be ready..."
	@sleep 5
	@curl -s -X POST http://$(CONNECT_HOST):$(CONNECT_PORT)/connectors \
		-H "Content-Type: application/json" \
		-d '{"name": "timescale-sink", "config": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "tasks.max": "1", "topics": "user_clicks", "connection.url": "jdbc:postgresql://timescaledb:5432/ecommerce_logs", "connection.user": "postgres", "connection.password": "password", "auto.create": "true", "insert.mode": "insert"}}' || echo "Connector might already exist."
	@echo "\n${GREEN}Setup Completed!${RESET}"

# ==============================================================================
# 🌊 4. CHẠY DEMO REAL-TIME (RUNTIME)
# ==============================================================================

## Terminal 1: Chạy Simulator (Giả lập người dùng click)
sim:
	@echo "${YELLOW}Running Simulation inside Docker...${RESET}"
	# Chạy producer với biến môi trường Kafka nội bộ
	docker exec -it -w /home/spark/work -e KAFKA_SERVER=kafka:29092 $(SPARK_MASTER) python3 src/simulation/main_producer.py

## Terminal 2: Chạy Spark Streaming (AI Inference Real-time)
stream:
	@echo "${YELLOW}Submitting Spark Streaming Job...${RESET}"
	# Submit job Spark để đọc Kafka và gọi Model AI
	docker exec -it -w /home/spark/work $(SPARK_MASTER) spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		--py-files /home/spark/work/src/processing/streaming/utils.py,/home/spark/work/src/ai_core/model.py \
		/home/spark/work/src/processing/streaming/inference.py

## Dọn dẹp dữ liệu rác (CẨN THẬN: Xóa sạch Database)
clean-data: down
	@echo "${YELLOW}Cleaning all data volumes...${RESET}"
	sudo rm -rf data/mongo_data/* data/timescale_data/* data/redis_data/*
	@echo "${GREEN}All data cleaned!${RESET}"
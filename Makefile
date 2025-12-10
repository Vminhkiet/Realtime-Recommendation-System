# ==============================================================================
# 🛠️ BIẾN MÔI TRƯỜNG
# ==============================================================================

DOCKER_COMPOSE = docker-compose
SPARK_MASTER   = spark-master
CONNECT_HOST   = localhost
CONNECT_PORT   = 8083

# Màu sắc
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
RESET  := $(shell tput -Txterm sgr0)

.PHONY: help up down setup stream sim ps

# ==============================================================================
# 🚀 LỆNH VẬN HÀNH (OPERATIONS)
# ==============================================================================

## 1. Bật hệ thống
up:
	@echo "$(YELLOW)Starting infrastructure...$(RESET)"
	$(DOCKER_COMPOSE) up -d

## 2. Tắt hệ thống
down:
	@echo "$(YELLOW)Stopping infrastructure...$(RESET)"
	$(DOCKER_COMPOSE) down

## 3. Xem danh sách container
ps:
	docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}"

# ==============================================================================
# ⚙️ KHỞI TẠO (SETUP - CHẠY 1 LẦN)
# ==============================================================================

## 4. Setup toàn bộ (Mongo + Timescale + Connect)
setup:
	@echo "$(YELLOW)--- 1. Importing Metadata to MongoDB ---$(RESET)"
	# [SỬA Ở ĐÂY] Đổi 'python' thành 'python3'
	docker exec -it -w /home/spark/work $(SPARK_MASTER) python3 src/utils/init_mongo.py

	# @echo "\n$(YELLOW)--- 2. Creating TimescaleDB Hypertable ---$(RESET)"
	# docker exec -i timescaledb psql -U postgres -d ecommerce_logs -c "\
	# CREATE TABLE IF NOT EXISTS user_activity (\
	#     time TIMESTAMPTZ NOT NULL,\
	#     user_id TEXT,\
	#     item_id TEXT,\
	#     action_type TEXT,\
	#     device TEXT\
	# ); \
	# SELECT create_hypertable('user_activity', 'time', if_not_exists => TRUE);"

	# @echo "\n$(GREEN)--- 3. Registering Kafka Connectors ---$(RESET)"
	# @curl -s -X POST http://$(CONNECT_HOST):$(CONNECT_PORT)/connectors \
	# -H "Content-Type: application/json" \
	# -d '{"name": "timescale-sink", "config": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "tasks.max": "1", "topics": "logs_analytics", "connection.url": "jdbc:postgresql://timescaledb:5432/ecommerce_logs", "connection.user": "postgres", "connection.password": "password", "auto.create": "true", "insert.mode": "insert"}}' > /dev/null

	# @echo "$(GREEN)Setup Completed!$(RESET)"

# ==============================================================================
# 🏃 CHẠY ỨNG DỤNG (RUNTIME)
# ==============================================================================

## 5. Chạy Spark Streaming (Mở Terminal 2)
stream:
	@echo "$(YELLOW)Submitting Spark Job...$(RESET)"
	docker exec -it $(SPARK_MASTER) spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		--py-files /home/spark/work/src/processing/streaming/utils.py,/home/spark/work/src/ai_core/model.py \
		/home/spark/work/src/processing/streaming/inference.py

## 6. Chạy Simulator (Mở Terminal 1)
sim:
	@echo "$(YELLOW)Running Simulation inside Docker...$(RESET)"
	docker exec -it $(SPARK_MASTER) python src/simulation/main_producer.py

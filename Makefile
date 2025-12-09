# --- MAKEFILE ĐÃ SỬA TÊN CONTAINER ---

# 1. Chạy Producer (Mở Terminal 1)
sim:
	python src/simulation/main_producer.py

# 2. Chạy Spark Streaming (Mở Terminal 2)
# Lưu ý: Tên container ở đây là 'spark-master' (ngắn gọn)
stream:
	docker exec -it spark-master spark-submit \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	\
	--py-files /home/spark/work/src/processing/streaming/utils.py,/home/spark/work/src/ai_core/model.py \
	\
	/home/spark/work/src/processing/streaming/inference.py

# 3. Xem danh sách container (Tiện ích)
ps:
	docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}"
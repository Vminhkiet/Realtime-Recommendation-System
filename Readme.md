sudo rm -rf data/model_registry/processed_parquet             data/model_registry/category_map.json             data/model_registry/item_map.json

sudo chmod 666 /var/run/docker.sock
docker exec -it airflow-scheduler bash
airflow dags trigger sasrec_recommender_system   --conf '{"start_date":"2025-11-01","end_date":"2025-11-8"}'
1. Phân chia mốc thời gian (Temporal Split)
Để mô phỏng đúng luồng thời gian thực tế, bạn nên chia như sau:

Tập Train (Huấn luyện): Từ 01/2018 đến hết 12/2022.

Đây là giai đoạn mô hình học các quy luật hành vi ổn định của người dùng.

Tập Test (Đánh giá): Toàn bộ năm 2023 (đến tháng 9 theo dữ liệu bạn có).

Bạn sẽ lấy món đồ cuối cùng của mỗi người dùng trong năm 2023 để làm "đề thi".

Simulation (Demo): Thời điểm hiện tại (2025).

{
	"event_id": {
		"string": "ac5214a8-a051-46df-8c75-e8ddac258ddf"
	},
	"session_id": {
		"string": "211c2dda-82eb-4f72-b5cd-723053c7fad1"
	},
	"item_id": {
		"string": "B0BVTJC24F"
	},
	"user_id": {
		"string": "AEWHHNPOZJEFFRRNOYM5JJ2GQSZQ"
	},
	"event_type": {
		"string": "purchase"
	},
	"rating_original": {
		"float": 5.0
	},
	"timestamp": {
		"string": "2025-12-01T02:42:35"
	},
	"context": {
		"ContextStruct": {
			"device": {
				"string": "desktop"
			},
			"location": {
				"string": "Fast_Sim"
			},
			"ip": {
				"string": "127.0.0.1"
			}
		}
	},
	"item_idx": 11893,
	"category_ids": 7,
	"processed_at": 1766919574
}
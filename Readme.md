sudo rm -rf data/model_registry/processed_parquet             data/model_registry/category_map.json             data/model_registry/item_map.json


1. Phân chia mốc thời gian (Temporal Split)
Để mô phỏng đúng luồng thời gian thực tế, bạn nên chia như sau:

Tập Train (Huấn luyện): Từ 01/2018 đến hết 12/2022.

Đây là giai đoạn mô hình học các quy luật hành vi ổn định của người dùng.

Tập Test (Đánh giá): Toàn bộ năm 2023 (đến tháng 9 theo dữ liệu bạn có).

Bạn sẽ lấy món đồ cuối cùng của mỗi người dùng trong năm 2023 để làm "đề thi".

Simulation (Demo): Thời điểm hiện tại (2025).
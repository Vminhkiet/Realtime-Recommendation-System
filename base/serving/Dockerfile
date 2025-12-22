# Sử dụng image Python nhẹ để tối ưu dung lượng
FROM python:3.9-slim

# Cài đặt các thư viện hệ thống cần thiết cho confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy file requirements trước để tận dụng Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn vào image
COPY . .

# Chạy script inference khi container khởi động
CMD ["python", "src/serving/online_inference.py"]
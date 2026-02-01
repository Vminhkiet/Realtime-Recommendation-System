FROM apache/airflow:2.9.0

USER root

# 1. Cài đặt Java (cho Spark) VÀ Docker CLI (để Airflow gọi Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Thiết lập biến môi trường
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 🔥 [QUAN TRỌNG NHẤT] Sửa lỗi 127 cho user Root
# Phải thêm ':/home/airflow/.local/bin' vào PATH
ENV PATH="${JAVA_HOME}/bin:/home/airflow/.local/bin:${PATH}"

# 3. Cấp quyền cho socket Docker (Phòng hờ lỗi permission)
RUN groupadd -f docker && usermod -aG docker airflow
RUN chmod +x /usr/bin/docker

# 4. Cài đặt thư viện Python
COPY infra/airflow/requirements.txt /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    # Fix lỗi Kafka cũ (nếu có dùng)
    sed -i "s|from kafka.vendor.six.moves import range|from six.moves import range|" \
    /home/airflow/.local/lib/python3.*/site-packages/kafka/codec.py || true
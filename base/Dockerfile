# 1. Dùng nền tảng của Alex Merced (đã có Spark, Java, Python)
FROM alexmerced/spark35nb:latest

# 2. Chuyển sang quyền Root để được phép cài phần mềm
USER root

# 3. Cài đặt các thư viện Python cần thiết cho đồ án
# (Đây là bước quan trọng để không phải cài lại mỗi lần bật máy)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 4. Thiết lập biến môi trường (Optional)
ENV SPARK_MASTER=spark://spark-master:7077
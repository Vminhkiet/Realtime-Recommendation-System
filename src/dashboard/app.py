import streamlit as st
import json
import pandas as pd
import os
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
import psycopg2

# --- 1. CẤU HÌNH & KẾT NỐI ---
st.set_page_config(layout="wide", page_title="Real-time RecSys", page_icon="🛒")

# Lấy biến môi trường từ docker-compose
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
TIMESCALE_URI = os.getenv('TIMESCALE_URI', "postgresql://postgres:password@localhost:5432/ecommerce_logs")
TOPIC_RECS = 'recommendations'

# --- 2. HÀM KẾT NỐI DATABASE (Cache để không kết nối lại nhiều lần) ---
@st.cache_resource
def init_connections():
    # A. Kết nối MongoDB (Metadata Sản phẩm)
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.server_info() # Trigger kiểm tra kết nối
        product_col = mongo_client["ecommerce_db"]["products"]
        print("✅ MongoDB Connected!")
    except Exception as e:
        print(f"❌ MongoDB Error: {e}")
        product_col = None

    # B. Kết nối TimescaleDB (Analytics)
    try:
        pg_conn = psycopg2.connect(TIMESCALE_URI)
        print("✅ TimescaleDB Connected!")
    except Exception as e:
        print(f"❌ TimescaleDB Error: {e}")
        pg_conn = None
        
    return product_col, pg_conn

product_col, pg_conn = init_connections()

# --- 3. CÁC HÀM TRUY VẤN DỮ LIỆU ---

def get_product_from_mongo(item_id):
    """Tra cứu thông tin sản phẩm từ MongoDB theo ASIN"""
    # [SỬA QUAN TRỌNG] Kiểm tra is None thay vì if not
    if product_col is None:
        return None
    
    # Tìm trong DB (Nhanh hơn đọc file JSON nhiều)
    try:
        item = product_col.find_one({"asin": item_id})
        if item:
            # Xử lý giá tiền (nếu lưu dạng string thì convert)
            price = item.get('price', 0)
            if price == 'Liên hệ' or price is None: price = 0
            return item
    except:
        pass
    
    return None

def get_traffic_stats():
    """Lấy thống kê click trong 1 giờ qua từ TimescaleDB"""
    # [SỬA QUAN TRỌNG] Kiểm tra is None
    if pg_conn is None:
        return pd.DataFrame()
    
    try:
        # Query SQL: Gom nhóm theo mỗi phút
        # query = """
        # SELECT time_bucket('1 minute', time) AS time_window, count(*) AS clicks 
        # FROM user_activity 
        # WHERE time > NOW() - INTERVAL '1 hour'
        # GROUP BY time_window 
        # ORDER BY time_window DESC 
        # LIMIT 20;
        # """
        query = """
        SELECT time_bucket('5 seconds', time) AS time_window, count(*) AS clicks 
        FROM user_activity 
        WHERE time > NOW() - INTERVAL '1 minute'
        GROUP BY time_window 
        ORDER BY time_window DESC 
        LIMIT 20;
        """
        # Dùng pandas đọc SQL trực tiếp
        df = pd.read_sql(query, pg_conn)
        return df
    except:
        return pd.DataFrame()

# --- 4. GIAO DIỆN CHÍNH (UI) ---

st.title("🛒 Hệ thống Gợi ý E-commerce Real-time (Enterprise)")
# [ĐÃ SỬA] Dòng hiển thị trạng thái dùng 'is not None'
st.markdown(f"**Status:** Kafka: `{KAFKA_SERVER}` | Mongo: `{'Online' if product_col is not None else 'Offline'}` | Timescale: `{'Online' if pg_conn is not None else 'Offline'}`")
st.markdown("---")

# Chia cột: Bên trái là Gợi ý (70%), Bên phải là Biểu đồ (30%)
col_main, col_stats = st.columns([7, 3])

rec_placeholder = col_main.empty()
chart_placeholder = col_stats.empty()

# --- 5. VÒNG LẶP XỬ LÝ KAFKA (MAIN LOOP) ---
try:
    consumer = KafkaConsumer(
        TOPIC_RECS,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest', 
        consumer_timeout_ms=1000
    )
except:
    st.error("❌ Không thể kết nối Kafka. Hãy kiểm tra lại container Kafka!")
    st.stop()

st.toast("Đang lắng nghe dữ liệu...")

if 'temp_stats' not in st.session_state:
    st.session_state['temp_stats'] = []

while True:
    # 1. Vẽ biểu đồ Traffic
    with chart_placeholder.container():
        st.subheader("📊 Traffic (1 Hour)")
        df_stats = get_traffic_stats()
        if not df_stats.empty:
            st.area_chart(df_stats.set_index('time_window'))
        else:
            st.info("Chưa có dữ liệu thống kê hành vi.")

    # 2. Đọc tin nhắn từ Kafka
    msg_pack = consumer.poll(timeout_ms=1000) 
    
    for tp, messages in msg_pack.items():
        for msg in messages:
            data = msg.value
            # [QUAN TRỌNG] Lấy cả 'user_id' và 'user' để tránh lỗi None
            user_id = data.get('user_id') or data.get('user')
            recs = data.get('recommendations', [])
            
            with rec_placeholder.container():
                st.success(f"🔔 Phát hiện User **{user_id}** vừa tương tác! Hệ thống gợi ý:")
                
                if not recs:
                    st.warning("AI chưa tìm ra sản phẩm phù hợp.")
                else:
                    cols = st.columns(4)
                    for i, item_id in enumerate(recs[:4]):
                        info = get_product_from_mongo(item_id)
                        
                        if not info:
                            info = {
                                "title": f"ID: {item_id}", 
                                "image": "https://via.placeholder.com/150?text=No+Data", 
                                "price": 0,
                                "store": "Unknown"
                            }

                        with cols[i]:
                            # Xử lý hiển thị ảnh an toàn
                            img_url = info.get('image')
                            if not img_url or not isinstance(img_url, str):
                                img_url = "https://via.placeholder.com/150?text=No+Image"

                            st.image(img_url, use_column_width=True)
                            st.caption(f"{info.get('title', 'No Name')[:40]}...")
                            st.markdown(f"**${info.get('price', 0)}**")
                            st.text(f"🏪 {info.get('store', 'Unknown')[:15]}")
                            if st.button("Chi tiết", key=f"{user_id}_{item_id}_{time.time()}"):
                                st.balloons()
                
                st.divider()

    time.sleep(1)
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
    if not product_col:
        return None
    
    # Tìm trong DB (Nhanh hơn đọc file JSON nhiều)
    item = product_col.find_one({"asin": item_id})
    if item:
        # Xử lý giá tiền (nếu lưu dạng string thì convert)
        price = item.get('price', 0)
        if price == 'Liên hệ' or price is None: price = 0
        return item
    
    return None

def get_traffic_stats():
    """Lấy thống kê click trong 1 giờ qua từ TimescaleDB"""
    if not pg_conn:
        return pd.DataFrame()
    
    try:
        # Query SQL: Gom nhóm theo mỗi phút
        query = """
        SELECT time_bucket('1 minute', time) AS time_window, count(*) AS clicks 
        FROM user_activity 
        WHERE time > NOW() - INTERVAL '1 hour'
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
st.markdown(f"**Status:** Kafka: `{KAFKA_SERVER}` | Mongo: `{'Online' if product_col is not None else 'Offline'}` | Timescale: `{'Online' if pg_conn is not None else 'Offline'}`")
st.markdown("---")

# Chia cột: Bên trái là Gợi ý (70%), Bên phải là Biểu đồ (30%)
col_main, col_stats = st.columns([7, 3])

# Placeholder để update dữ liệu mà không cần refresh cả trang
rec_placeholder = col_main.empty()
chart_placeholder = col_stats.empty()

# --- 5. VÒNG LẶP XỬ LÝ KAFKA (MAIN LOOP) ---
# Khởi tạo Consumer
try:
    consumer = KafkaConsumer(
        TOPIC_RECS,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest', # Chỉ lấy tin mới nhất
        consumer_timeout_ms=1000    # Chờ 1s nếu không có tin thì lặp lại vòng while
    )
except:
    st.error("❌ Không thể kết nối Kafka. Hãy kiểm tra lại container Kafka!")
    st.stop()

st.toast("Đang lắng nghe dữ liệu...")

# Biến lưu trữ tạm để vẽ biểu đồ nếu Timescale chưa có data
if 'temp_stats' not in st.session_state:
    st.session_state['temp_stats'] = []

# Vòng lặp chính của Streamlit (thay thế cho Thread)
while True:
    # 1. Vẽ biểu đồ Traffic (Bên phải)
    with chart_placeholder.container():
        st.subheader("📊 Traffic (1 Hour)")
        df_stats = get_traffic_stats()
        if not df_stats.empty:
            st.area_chart(df_stats.set_index('time_window'))
        else:
            st.info("Chưa có dữ liệu thống kê hành vi.")

    # 2. Đọc tin nhắn từ Kafka (Bên trái)
    # consumer sẽ trả về 1 mảng các tin nhắn mới nhận được
    msg_pack = consumer.poll(timeout_ms=1000) 
    
    for tp, messages in msg_pack.items():
        for msg in messages:
            data = msg.value
            user_id = data.get('user_id') or data.get('user')
            recs = data.get('recs', [])
            
            # Hiển thị Gợi ý ra màn hình
            with rec_placeholder.container():
                st.success(f"🔔 Phát hiện User **{user_id}** vừa tương tác! Hệ thống gợi ý:")
                
                if not recs:
                    st.warning("AI chưa tìm ra sản phẩm phù hợp.")
                else:
                    cols = st.columns(4) # Hiển thị 4 sản phẩm hàng ngang
                    for i, item_id in enumerate(recs[:4]):
                        # LẤY DATA TỪ MONGODB
                        info = get_product_from_mongo(item_id)
                        
                        # Fallback nếu không tìm thấy trong Mongo
                        if not info:
                            info = {
                                "title": f"ID: {item_id}", 
                                "image": "https://via.placeholder.com/150?text=No+Data", 
                                "price": 0,
                                "store": "Unknown"
                            }

                        with cols[i]:
                            st.image(info['image'], use_column_width=True)
                            st.caption(f"{info['title'][:40]}...")
                            st.markdown(f"**${info['price']}**")
                            st.text(f"🏪 {info.get('store', '')[:15]}")
                            if st.button("Chi tiết", key=f"{user_id}_{item_id}_{time.time()}"):
                                st.balloons()
                
                # Thêm đường kẻ phân cách các lần gợi ý
                st.divider()

    # Nghỉ 1 xíu để không ăn hết CPU
    time.sleep(1)
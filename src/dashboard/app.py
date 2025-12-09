import streamlit as st
import json
import os
import time
import threading
import uuid
from kafka import KafkaConsumer
# Thư viện để fix lỗi cảnh báo Thread trong Streamlit
from streamlit.runtime.scriptrunner import add_script_run_ctx

# --- 1. CẤU HÌNH HỆ THỐNG ---
# Lấy cấu hình từ biến môi trường (nếu chạy Docker), mặc định localhost (nếu chạy tay)
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
TOPIC_OUT = 'recommendations'
META_FILE = os.getenv('META_FILE_PATH', 'data/raw_source/meta_All_Beauty.jsonl')

# --- 2. HÀM LOAD DATA SẢN PHẨM (METADATA) ---
@st.cache_data
def load_product_catalog():
    """
    Đọc file Metadata để lấy Tên, Giá, Ảnh sản phẩm.
    Dữ liệu được Cache vào RAM để không phải đọc lại nhiều lần.
    """
    products = {}
    if not os.path.exists(META_FILE):
        return {}
    
    with open(META_FILE, 'r') as f:
        for line in f:
            try:
                p = json.loads(line)
                # Amazon có thể dùng asin hoặc parent_asin
                asin = p.get('asin') or p.get('parent_asin')
                
                # Lấy ảnh đẹp nhất (Large > Thumb), nếu không có thì dùng ảnh giữ chỗ
                img = "https://via.placeholder.com/150?text=No+Image"
                if p.get('images') and len(p['images']) > 0:
                    img = p['images'][0].get('large', img)
                
                products[asin] = {
                    "title": p.get('title', f'Unknown Product ({asin})'),
                    "price": p.get('price', 'Liên hệ'),
                    "image": img,
                    "brand": p.get('store', 'Unknown Brand')
                }
            except: continue
    return products

# Khởi tạo Catalog vào Session State
if 'catalog' not in st.session_state:
    st.session_state['catalog'] = load_product_catalog()

# --- 3. KAFKA CONSUMER (CHẠY NGẦM) ---
if 'messages' not in st.session_state:
    st.session_state['messages'] = []

def kafka_listener():
    """
    Luồng riêng để liên tục lắng nghe tin nhắn từ Kafka
    """
    try:
        print(f"🔌 Đang kết nối Kafka tại: {KAFKA_SERVER}")
        consumer = KafkaConsumer(
            TOPIC_OUT,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',       # Đọc từ tin nhắn cũ nhất
            group_id=f'dashboard_{uuid.uuid4()}', # Tạo group mới để không bị nhớ vị trí cũ
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✅ Kafka Consumer đã kết nối!")

        for msg in consumer:
            # Nhận tin nhắn mới và đẩy vào đầu danh sách hiển thị
            st.session_state['messages'].insert(0, msg.value)
            
            # Chỉ giữ lại 10 tin mới nhất để giao diện không bị lag
            if len(st.session_state['messages']) > 10:
                st.session_state['messages'] = st.session_state['messages'][:10]
            
            # Nghỉ một chút để giảm tải CPU
            time.sleep(0.1)
            
    except Exception as e:
        print(f"❌ Kafka Error (Có thể bỏ qua nếu đang chờ tin): {e}")

# Khởi động Thread lắng nghe Kafka (Chỉ chạy 1 lần)
if 'thread_started' not in st.session_state:
    t = threading.Thread(target=kafka_listener, daemon=True)
    add_script_run_ctx(t) # <--- DÒNG QUAN TRỌNG ĐỂ FIX CẢNH BÁO
    t.start()
    st.session_state['thread_started'] = True

# --- 4. GIAO DIỆN NGƯỜI DÙNG (UI) ---
st.set_page_config(layout="wide", page_title="Real-time RecSys", page_icon="🛒")

st.title("🛒 Hệ thống Gợi ý E-commerce Real-time")
st.markdown("---")

# Chia màn hình thành 2 cột: Log bên trái, Sản phẩm bên phải
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("📡 Dữ liệu từ Spark (Live Log)")
    
    # Nút làm mới thủ công (Streamlit cần tương tác để vẽ lại UI từ background thread)
    if st.button('🔄 Cập nhật màn hình'):
        pass
    
    if st.session_state['messages']:
        latest = st.session_state['messages'][0]
        st.success(f"User đang hoạt động: **{latest.get('user_id')[:15]}...**")
        st.json(latest, expanded=False)
    else:
        st.info("Chưa có dữ liệu. Vui lòng kiểm tra Producer...")

with col2:
    st.subheader("🎯 Sản phẩm Gợi ý (Recommendation)")
    
    if st.session_state['messages']:
        latest_msg = st.session_state['messages'][0]
        recs = latest_msg.get('recommendations', [])
        catalog = st.session_state['catalog']
        
        if not recs:
            st.warning("AI không tìm thấy gợi ý nào phù hợp.")
        else:
            # Hiển thị 3 sản phẩm đầu tiên
            cols = st.columns(3)
            for i, item_id in enumerate(recs[:3]):
                # Tra cứu thông tin sản phẩm
                info = catalog.get(item_id, {
                    "title": f"ID: {item_id} (Thiếu Info)", 
                    "image": "https://via.placeholder.com/150", 
                    "price": "?",
                    "brand": "?"
                })
                
                with cols[i]:
                    st.image(info['image'], use_column_width=True)
                    st.markdown(f"**{info['title'][:50]}...**")
                    st.caption(f"Hãng: {info['brand']}")
                    st.markdown(f"💰 **{info['price']}**")
                    if st.button(f"Mua ngay", key=f"btn_{i}_{item_id}"):
                        st.balloons()
    else:
        st.warning("Đang chờ dữ liệu từ hệ thống...")
        st.spinner("Waiting for events from Spark Streaming...")

# Tự động refresh giao diện mỗi 2 giây để cập nhật data mới từ Thread
time.sleep(2)
st.rerun()
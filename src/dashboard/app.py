import streamlit as st
import pandas as pd
import psycopg2
import redis
import pymongo
import json
import plotly.express as px
import os

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
st.set_page_config(
    page_title="RecSys Command Center", 
    page_icon="🚀", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- A. Cấu hình Redis (Real-time Context) ---
REDIS_HOST = os.getenv('REDIS_HOST', 'redis') 
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# --- B. Cấu hình TimescaleDB (Analytics) ---
DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ecommerce_logs')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'password')
DB_DSN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- C. Cấu hình MongoDB (Product Metadata) ---
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
MONGO_DB = "ecommerce_db"
MONGO_COL_PRODUCTS = "products"

# File Map: Dùng để dịch Index (số) -> ItemID (chuỗi)
ITEM_MAP_PATH = "./data/item_map.json"

# ==========================================
# 2. HÀM KẾT NỐI (CONNECTION POOL)
# ==========================================

@st.cache_resource(ttl=5) 
def get_redis_client():
    try:
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True, socket_timeout=1)
        client.ping() 
        return client
    except Exception: return None

@st.cache_resource
def get_mongo_client():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.server_info() 
        return client
    except Exception: return None

def get_db_connection():
    try:
        return psycopg2.connect(DB_DSN)
    except Exception: return None

@st.cache_data
def load_index_map():
    """
    Load file map: {"0": "B00123...", "1": "B00XYZ..."}
    Trả về dict map từ Index -> ASIN
    """
    if os.path.exists(ITEM_MAP_PATH):
        try:
            with open(ITEM_MAP_PATH, 'r') as f:
                raw = json.load(f)
            # Đảm bảo key là string để dễ lookup
            return {str(k): str(v) for k, v in raw.items()}
        except: return {}
    return {}

# Load map ngay khi khởi động
index_to_id_map = load_index_map()

# ==========================================
# 3. CORE: LẤY CHI TIẾT SẢN PHẨM TỪ MONGO
# ==========================================
def fetch_product_details(item_ids_list):
    """
    Input: List các Item ID (ASIN) (VD: ['B00123', 'B00456'])
    Output: Dict {ItemID: {title, image, rating, price}}
    """
    mongo = get_mongo_client()
    if not mongo or not item_ids_list:
        return {}
    
    db = mongo[MONGO_DB]
    col = db[MONGO_COL_PRODUCTS]
    
    # Query MongoDB: Tìm các document có asin hoặc parent_asin nằm trong list
    cursor = col.find({
        "$or": [
            {"asin": {"$in": item_ids_list}},
            {"parent_asin": {"$in": item_ids_list}}
        ]
    })
    
    product_info = {}
    for doc in cursor:
        # Lấy info chuẩn hóa
        info = {
            "title": doc.get("title", "Unknown Product"),
            "image": doc.get("image", "https://via.placeholder.com/150?text=No+Image"),
            "rating": doc.get("average_rating", 0),
            "price": doc.get("price", 0),
            "category": doc.get("main_category", "General")
        }
        
        # Map vào cả asin và parent_asin để tìm kiểu gì cũng thấy
        if doc.get("asin"): product_info[doc["asin"]] = info
        if doc.get("parent_asin"): product_info[doc["parent_asin"]] = info
        
    return product_info

# ==========================================
# 4. GIAO DIỆN DASHBOARD
# ==========================================

st.title("⚡ Real-time AI Recommendation System")
st.markdown(f"**System Status:** `Redis@{REDIS_HOST}` | `Mongo@{MONGO_HOST}` | `Timescale@{DB_HOST}`")

# --- KPI METRICS ---
r_client = get_redis_client()
m_client = get_mongo_client()
db_conn = get_db_connection()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Redis Cache", "✅ ONLINE" if r_client else "❌ OFFLINE")
c2.metric("MongoDB Meta", "✅ ONLINE" if m_client else "❌ OFFLINE")
c3.metric("TimescaleDB", "✅ ONLINE" if db_conn else "❌ OFFLINE")

# Đếm Traffic nhanh
total_events = 0
if db_conn:
    try:
        cur = db_conn.cursor()
        cur.execute("SELECT count(*) FROM user_interactions WHERE timestamp > NOW() - INTERVAL '24 hours'")
        total_events = cur.fetchone()[0]
        cur.close()
        db_conn.close()
    except: pass
c4.metric("Traffic (24h)", f"{total_events:,}")

st.divider()

# --- TABS CHỨC NĂNG ---
tab_inspector, tab_analytics = st.tabs(["🕵️ User Inspector (Live)", "📈 Market Trends (Analytics)"])

# =========================================================
# TAB 1: USER INSPECTOR (REDIS + MAP + MONGO)
# =========================================================
with tab_inspector:
    col_input, _ = st.columns([1, 3])
    with col_input:
        st.subheader("Simulate User Context")
        user_id = st.text_input("User ID:", value="AHJRJCJMK3XVV4BSPBRAHIYEODWA")
        if st.button("🔄 Refresh Real-time Data", type="primary"):
            st.rerun()

    if not r_client:
        st.error("⚠️ Lỗi kết nối Redis. Hãy kiểm tra container.")
    else:
        col_hist, col_rec = st.columns(2)
        
        # --- A. LỊCH SỬ XEM (HISTORY) ---
        with col_hist:
            st.info("📚 **History Context** (Redis)")
            hist_key = f"hist:{user_id}"
            
            if r_client.exists(hist_key):
                # 1. Lấy Index từ Redis (VD: [102, 55])
                hist_indices = r_client.lrange(hist_key, 0, -1)
                
                # 2. Dịch Index -> ASIN (B00...)
                real_ids = []
                for idx in hist_indices:
                    mapped_id = index_to_id_map.get(str(idx), "N/A")
                    real_ids.append(mapped_id)
                
                # 3. Lấy tên từ MongoDB
                details = fetch_product_details(real_ids)
                
                # 4. Hiển thị bảng
                table_data = []
                for idx, r_id in zip(hist_indices, real_ids):
                    info = details.get(r_id, {})
                    p_name = info.get('title', '❌ Mongo không tìm thấy')
                    if r_id == "N/A": p_name = "⚠️ Lỗi Map Index"
                        
                    table_data.append({
                        "Index": idx,
                        "ASIN": r_id,
                        "Product Name": p_name
                    })
                
                st.caption(f"User xem {len(hist_indices)} sản phẩm gần đây:")
                # Đảo ngược để xem mới nhất
                st.table(pd.DataFrame(table_data).iloc[::-1])
            else:
                st.warning("User chưa có lịch sử.")

        # --- B. GỢI Ý AI (RECOMMENDATION) ---
        with col_rec:
            st.success("💎 **AI Recommendations** (SASRec)")
            rec_key = f"rec:{user_id}"
            
            if r_client.exists(rec_key):
                # 1. Lấy List Index
                rec_json = r_client.get(rec_key)
                rec_indices = json.loads(rec_json)[:5]
                
                # 2. Dịch Index -> ASIN
                rec_real_ids = [index_to_id_map.get(str(idx), str(idx)) for idx in rec_indices]
                
                # 3. Lấy Info từ Mongo
                details = fetch_product_details(rec_real_ids)
                
                # 4. Hiển thị Card
                st.caption("Model dự đoán User sẽ thích:")
                for idx, r_id in enumerate(rec_real_ids):
                    info = details.get(r_id, {"title": f"ID: {r_id}", "image": None})
                    
                    with st.container(border=True):
                        c_rank, c_img, c_info = st.columns([0.5, 1, 3])
                        with c_rank: st.markdown(f"### #{idx+1}")
                        with c_img:
                            # Dùng use_column_width để tương thích mọi bản Streamlit
                            if info['image']: st.image(info['image'], use_column_width=True)
                            else: st.text("No Img")
                        with c_info:
                            st.markdown(f"**{info['title'][:60]}...**")
                            st.caption(f"⭐ {info.get('rating',0)} | 💵 ${info.get('price',0)}")
                            st.code(r_id)
            else:
                st.warning("Chưa có gợi ý.")

# =========================================================
# TAB 2: ANALYTICS (TIMESCALE DB)
# =========================================================
with tab_analytics:
    st.subheader("📊 Market Trends & Traffic")
    
    conn = get_db_connection()
    if not conn:
        st.error("⚠️ Không kết nối được TimescaleDB.")
    else:
        # Query 1: Top Trending
        query_trend = """
            SELECT item_id, SUM(interaction_count) as total_views
            FROM top_trending_items
            WHERE bucket > NOW() - INTERVAL '24 hours'
            GROUP BY item_id
            ORDER BY total_views DESC
            LIMIT 10
        """
        df_trend = pd.read_sql(query_trend, conn)
        
        # Query 2: Traffic
        query_traffic = """
            SELECT time_bucket('10 minutes', timestamp) as time_period, count(*) as events
            FROM user_interactions
            WHERE timestamp > NOW() - INTERVAL '12 hours'
            GROUP BY time_period
            ORDER BY time_period
        """
        df_traffic = pd.read_sql(query_traffic, conn)
        conn.close()

        col_left, col_right = st.columns(2)
        
        with col_left:
            st.markdown("**🔥 Top 10 Sản phẩm Hot (24h)**")
            if not df_trend.empty:
                # Lấy tên sản phẩm cho đẹp
                ids = df_trend['item_id'].tolist()
                infos = fetch_product_details(ids)
                df_trend['name'] = df_trend['item_id'].apply(lambda x: infos.get(x, {}).get('title', x)[:30])
                
                fig_bar = px.bar(
                    df_trend, x='total_views', y='name', orientation='h', 
                    color='total_views', color_continuous_scale='Viridis'
                )
                fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu xu hướng.")

        with col_right:
            st.markdown("**🌊 Lưu lượng truy cập (12h qua)**")
            if not df_traffic.empty:
                fig_line = px.area(
                    df_traffic, x='time_period', y='events', 
                    line_shape='spline', color_discrete_sequence=['#00CC96']
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu traffic.")
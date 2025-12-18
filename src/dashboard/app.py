import streamlit as st
import json
import pandas as pd
import os
import time

# Cấu hình trang
st.set_page_config(page_title="AI Training Monitor", layout="wide")

st.title("📊 Realtime AI Recommendation Training Dashboard")

LOG_FILE = "/home/spark/work/data/training_history.json"

# Hàm load dữ liệu
def load_data():
    if not os.path.exists(LOG_FILE):
        return None
    with open(LOG_FILE, 'r') as f:
        return json.load(f)

# Nút làm mới dữ liệu
if st.button('🔄 Làm mới dữ liệu'):
    st.rerun()

data = load_data()

if data is None:
    st.warning("⚠️ Chưa tìm thấy file log training. Hãy chạy lệnh training trước!")
else:
    # Tạo DataFrame từ JSON
    df = pd.DataFrame(data)
    df['epoch'] = df.index + 1 # Thêm cột Epoch bắt đầu từ 1
    
    # Layout 2 cột
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📉 Biểu đồ Loss (Sai số)")
        # Chỉ lấy các cột liên quan đến Loss
        loss_cols = ['loss']
        if 'val_loss' in df.columns:
            loss_cols.append('val_loss')
        st.line_chart(df[loss_cols])
        
    with col2:
        st.subheader("📈 Biểu đồ Accuracy (Độ chính xác)")
        # Lấy các cột không phải loss và epoch
        acc_cols = [c for c in df.columns if 'loss' not in c and c != 'epoch']
        if acc_cols:
            st.line_chart(df[acc_cols])
        else:
            st.info("Model này không log Accuracy.")

    st.write("### 📝 Dữ liệu chi tiết")
    st.dataframe(df)
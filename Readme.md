graph LR
    %% Định nghĩa màu sắc cho đẹp
    classDef source fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;
    classDef speed fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef batch fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    classDef serving fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px;
    classDef storage fill:#fff,stroke:#333,stroke-dasharray: 5 5;

    %% 1. INGESTION LAYER
    subgraph SOURCE [1. INGESTION LAYER (Nguồn Dữ Liệu)]
        direction TB
        RawFile(File Amazon JSON):::source
        Sim[Python Simulator<br/>(main_producer.py)]:::source
        KafkaIn[("Kafka Topic:<br/>user_clicks")]:::source
    end

    %% 2. SPEED LAYER
    subgraph SPEED [2. SPEED LAYER (Xử lý Thời gian thực)]
        direction TB
        SparkS[Spark Streaming<br/>(inference.py)]:::speed
        Redis[("Redis<br/>(Lưu Session/History)")]:::storage
        AI_Run[[AI Model .keras<br/>(Đang chạy)]]:::speed
        KafkaOut[("Kafka Topic:<br/>recommendations")]:::speed
    end

    %% 3. BATCH LAYER
    subgraph BATCH [3. BATCH LAYER (Huấn luyện & Lưu trữ)]
        direction TB
        MinIO[("Data Lake MinIO<br/>(Bronze/Silver/Gold)")]:::batch
        Train[Training Job<br/>(train.py)]:::batch
        AI_New[[Model Mới<br/>(Retrained)]]:::batch
    end

    %% 4. SERVING LAYER
    subgraph APP [4. SERVING LAYER (Hiển thị)]
        direction TB
        Dashboard[Streamlit Web App<br/>(app.py)]:::serving
        MetaFile(File Metadata<br/>Ảnh/Giá/Tên):::storage
    end

    %% --- CÁC ĐƯỜNG KẾT NỐI ---
    
    %% Luồng Ingestion
    RawFile --> Sim
    Sim -->|Bắn log giả lập| KafkaIn

    %% Luồng Speed
    KafkaIn -->|Đọc Stream| SparkS
    SparkS <-->|Lấy lịch sử User| Redis
    SparkS -->|Dự đoán| AI_Run
    SparkS -->|Kết quả Gợi ý| KafkaOut

    %% Luồng Batch
    KafkaIn -.->|Lưu trữ lâu dài| MinIO
    MinIO -->|Load Data| Train
    Train -->|Cập nhật trọng số| AI_New
    AI_New -.->|Thay thế định kỳ| AI_Run

    %% Luồng Serving
    KafkaOut -->|Hiển thị Real-time| Dashboard
    MetaFile -.->|Tra cứu thông tin SP| Dashboard

    %% Chú thích kết nối
    linkStyle 0,1,2,3,4,5,6,7,8,9,10,11 stroke-width:2px,fill:none,stroke:gray;
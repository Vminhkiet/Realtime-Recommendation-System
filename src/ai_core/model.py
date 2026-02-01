import keras
import keras_hub
from keras import ops
import tensorflow as tf

@keras.saving.register_keras_serializable()
class SasRec(keras.Model):
    def __init__(self, vocabulary_size, category_size, num_layers, num_heads, hidden_dim, dropout=0.1, max_sequence_length=50, **kwargs):
        super().__init__(**kwargs)
        self.vocabulary_size = vocabulary_size
        self.category_size = category_size
        self.num_layers = num_layers
        self.num_heads = num_heads
        self.hidden_dim = hidden_dim
        self.dropout = dropout
        self.max_sequence_length = max_sequence_length

        # 1. Embeddings
        self.item_embedding = keras_hub.layers.ReversibleEmbedding(
            input_dim=vocabulary_size, 
            output_dim=hidden_dim, 
            embeddings_initializer="glorot_uniform",
            name="item_emb"
        )
        
        self.category_embedding = keras.layers.Embedding(
            input_dim=category_size + 1,
            output_dim=hidden_dim,
            embeddings_initializer="glorot_uniform",
            name="category_emb"
        )

        self.position_embedding = keras_hub.layers.PositionEmbedding(
            sequence_length=max_sequence_length, 
            initializer="glorot_uniform",
            name="pos_emb"
        )
        
        # 🔥 TỐI ƯU 1: GATING NETWORK (Mạng tính trọng số pha trộn)
        # Đầu vào là [Item_Emb, Cat_Emb] -> Đầu ra là tỷ lệ alpha (0 đến 1)
        self.feature_gate = keras.layers.Dense(hidden_dim, activation="sigmoid", name="feature_gate")

        self.embeddings_dropout = keras.layers.Dropout(dropout)

        # 2. Encoder Layers
        self.transformer_layers = []
        for _ in range(num_layers):
            self.transformer_layers.append(
                keras_hub.layers.TransformerDecoder(
                    intermediate_dim=hidden_dim * 4, 
                    num_heads=num_heads, 
                    dropout=dropout, 
                    activation="relu", 
                    normalize_first=True 
                )
            )

        self.layer_norm = keras.layers.LayerNormalization(axis=-1, epsilon=1e-6)

    def call(self, inputs, training=False):
        item_ids = inputs["item_ids"]
        category_ids = inputs["category_ids"]
        padding_mask = inputs["padding_mask"]
        
        # 1. Lấy vector thô
        x_item = self.item_embedding(item_ids)      # [Batch, Seq, Dim]
        x_cat = self.category_embedding(category_ids) # [Batch, Seq, Dim]
        x_pos = self.position_embedding(x_item)     # [Batch, Seq, Dim]
        
        # 🔥 TỐI ƯU 1: ÁP DỤNG GATING MECHANISM
        # Thay vì cộng: x = x_item + x_cat
        # Chúng ta dùng cổng:
        # Tính cổng alpha dựa trên sự kết hợp của cả 2
        gate_input = ops.concatenate([x_item, x_cat], axis=-1)
        alpha = self.feature_gate(x_item) # Hoặc dùng x_item làm tín hiệu điều khiển
        
        # Công thức pha trộn: (alpha * Item) + ((1-alpha) * Category)
        # Ý nghĩa: Nếu Item lạ quá, cổng alpha sẽ nhỏ đi để ưu tiên Category
        x_features = (alpha * x_item) + ((1.0 - alpha) * x_cat)
        
        # Cộng Position như bình thường
        x = x_features + x_pos
        x = self.embeddings_dropout(x)
        
        # Qua Transformer Layers
        for layer in self.transformer_layers:
            x = layer(x, decoder_padding_mask=padding_mask)
        
        x = self.layer_norm(x)
        
        result = {"item_sequence_embedding": x}
        
        if not training:
            valid_mask = ops.cast(ops.logical_not(padding_mask), "int32")
            last_indices = ops.maximum(ops.sum(valid_mask, axis=1) - 1, 0)
            
            last_emb = ops.take_along_axis(x, ops.expand_dims(last_indices, -1)[:, :, None], axis=1)
            last_emb = ops.squeeze(last_emb, axis=1)
            
            scores = ops.matmul(last_emb, ops.transpose(self.item_embedding.embeddings))
            _, top_k = tf.math.top_k(scores, k=10)
            result["predictions"] = top_k
            
        return result

    def compute_loss(self, x, y, y_pred, sample_weight=None):
        """
        🔥 BPR LOSS + TIME REWEIGHTING + LABEL SMOOTHING 🔥
        """
        seq_emb = y_pred["item_sequence_embedding"]
        pos_emb = self.item_embedding(y["positive_sequence"])
        neg_emb = self.item_embedding(y["negative_sequence"])
        
        pos_logits = ops.sum(pos_emb * seq_emb, axis=-1)
        neg_logits = ops.sum(neg_emb * seq_emb, axis=-1)
        
        pos_loss = -ops.log(tf.nn.sigmoid(pos_logits) + 1e-8) * 0.9
        neg_loss = -ops.log(1.0 - tf.nn.sigmoid(neg_logits) + 1e-8)
        # 🔥 TỐI ƯU 2: LABEL SMOOTHING (GIẢ LẬP) TRONG BPR
        # Thay vì ép hiệu số phải cực lớn, ta chấp nhận một độ nhiễu nhỏ
        # Giúp model không bị Overfitting vào các mẫu nhiễu
        loss = pos_loss + neg_loss
        
        # Masking
        mask = ops.cast(x["padding_mask"], dtype="float32")
        loss = loss * mask
        # Time Reweighting (Giữ nguyên từ bước trước)
        seq_len = ops.shape(loss)[1]
        time_weights = tf.range(1, seq_len + 1, dtype=tf.float32)
        time_weights = time_weights / tf.cast(seq_len, dtype=tf.float32)
        time_weights = ops.expand_dims(time_weights, 0)

        loss = loss * time_weights
        if sample_weight is not None:
            loss = loss * sample_weight
        
        normalization_factor = ops.sum(mask * time_weights) + 1e-8

        return ops.sum(loss) / normalization_factor

    def get_config(self):
        return {
            "vocabulary_size": self.vocabulary_size,
            "category_size": self.category_size,
            "num_layers": self.num_layers,
            "num_heads": self.num_heads, 
            "hidden_dim": self.hidden_dim,
            "dropout": self.dropout, 
            "max_sequence_length": self.max_sequence_length
        }
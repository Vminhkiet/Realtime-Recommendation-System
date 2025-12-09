import keras
import keras_hub
from keras import ops
import tensorflow as tf

# Decorator này BẮT BUỘC để Keras biết cách lưu/load class tùy chỉnh
@keras.saving.register_keras_serializable()
class SasRec(keras.Model):
    def __init__(
        self,
        vocabulary_size,
        num_layers,
        num_heads,
        hidden_dim,
        dropout=0.0,
        max_sequence_length=100,
        dtype=None,
        **kwargs,
    ):
        super().__init__(dtype=dtype, **kwargs)

        # Lưu các tham số vào self để dùng cho get_config
        self.vocabulary_size = vocabulary_size
        self.num_layers = num_layers
        self.num_heads = num_heads
        self.hidden_dim = hidden_dim
        self.dropout = dropout
        self.max_sequence_length = max_sequence_length

        # ======== Layers ========

        # === Embeddings ===
        self.item_embedding = keras_hub.layers.ReversibleEmbedding(
            input_dim=vocabulary_size,
            output_dim=hidden_dim,
            embeddings_initializer="glorot_uniform",
            embeddings_regularizer=keras.regularizers.l2(0.001),
            dtype=dtype,
            name="item_embedding",
        )
        self.position_embedding = keras_hub.layers.PositionEmbedding(
            initializer="glorot_uniform",
            sequence_length=max_sequence_length,
            dtype=dtype,
            name="position_embedding",
        )
        self.embeddings_add = keras.layers.Add(dtype=dtype, name="embeddings_add")
        self.embeddings_dropout = keras.layers.Dropout(dropout, dtype=dtype, name="embeddings_dropout")

        # === Decoder layers ===
        self.transformer_layers = []
        for i in range(num_layers):
            self.transformer_layers.append(
                keras_hub.layers.TransformerDecoder(
                    intermediate_dim=hidden_dim,
                    num_heads=num_heads,
                    dropout=dropout,
                    layer_norm_epsilon=1e-05,
                    activation="relu",
                    kernel_initializer="glorot_uniform",
                    normalize_first=True,
                    dtype=dtype,
                    name=f"transformer_layer_{i}",
                )
            )

        # === Final layer norm ===
        self.layer_norm = keras.layers.LayerNormalization(
            axis=-1, epsilon=1e-8, dtype=dtype, name="layer_norm"
        )

        # === Loss ===
        self.loss_fn = keras.losses.BinaryCrossentropy(from_logits=True, reduction=None)

    # --- HÀM QUAN TRỌNG ĐỂ SỬA LỖI LƯU MODEL ---
    def get_config(self):
        config = super().get_config()
        config.update({
            "vocabulary_size": self.vocabulary_size,
            "num_layers": self.num_layers,
            "num_heads": self.num_heads,
            "hidden_dim": self.hidden_dim,
            "dropout": self.dropout,
            "max_sequence_length": self.max_sequence_length,
        })
        return config

    @classmethod
    def from_config(cls, config):
        return cls(**config)
    # -------------------------------------------

    def _get_last_non_padding_token(self, tensor, padding_mask):
        valid_token_mask = ops.logical_not(padding_mask)
        seq_lengths = ops.sum(ops.cast(valid_token_mask, "int32"), axis=1)
        last_token_indices = ops.maximum(seq_lengths - 1, 0)
        indices = ops.expand_dims(last_token_indices, axis=(-2, -1))
        gathered_tokens = ops.take_along_axis(tensor, indices, axis=1)
        last_token_embedding = ops.squeeze(gathered_tokens, axis=1)
        return last_token_embedding

    def build(self, input_shape):
        embedding_shape = list(input_shape) + [self.hidden_dim]
        self.item_embedding.build(input_shape)
        self.position_embedding.build(embedding_shape)
        self.embeddings_add.build((embedding_shape, embedding_shape))
        self.embeddings_dropout.build(embedding_shape)
        for transformer_layer in self.transformer_layers:
            transformer_layer.build(decoder_sequence_shape=embedding_shape)
        self.layer_norm.build(embedding_shape)
        super().build(input_shape)

    def call(self, inputs, training=False):
        item_ids, padding_mask = inputs["item_ids"], inputs["padding_mask"]
        x = self.item_embedding(item_ids)
        position_embedding = self.position_embedding(x)
        x = self.embeddings_add((x, position_embedding))
        x = self.embeddings_dropout(x)
        for transformer_layer in self.transformer_layers:
            x = transformer_layer(x, decoder_padding_mask=padding_mask)
        item_sequence_embedding = self.layer_norm(x)
        
        result = {"item_sequence_embedding": item_sequence_embedding}
        
        # Logic Inference (Tự tính Top-K thay vì dùng thư viện ngoài để tránh lỗi version)
        if not training:
            last_item_embedding = self._get_last_non_padding_token(item_sequence_embedding, padding_mask)
            # Lấy toàn bộ bảng vector sản phẩm
            all_items = self.item_embedding.embeddings
            # Tính điểm (Dot Product)
            scores = ops.matmul(last_item_embedding, ops.transpose(all_items))
            # Lấy Top 10
            top_k_scores, top_k_indices = tf.math.top_k(scores, k=10)
            result["predictions"] = top_k_indices
            
        return result

    def compute_loss(self, x, y, y_pred, sample_weight, training=False):
        item_sequence_embedding = y_pred["item_sequence_embedding"]
        positive_embedding = self.item_embedding(y["positive_sequence"])
        negative_embedding = self.item_embedding(y["negative_sequence"])
        
        positive_logits = ops.sum(ops.multiply(positive_embedding, item_sequence_embedding), axis=-1)
        negative_logits = ops.sum(ops.multiply(negative_embedding, item_sequence_embedding), axis=-1)
        
        logits = ops.concatenate([positive_logits, negative_logits], axis=1)
        labels = ops.concatenate([ops.ones_like(positive_logits), ops.zeros_like(negative_logits)], axis=1)
        
        return self.loss_fn(y_true=labels, y_pred=logits)
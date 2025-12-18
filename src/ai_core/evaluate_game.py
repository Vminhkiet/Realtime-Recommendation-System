import os
import json
import pickle
import numpy as np
import tensorflow as tf
from tqdm import tqdm

try:
    from .model import SasRec
except ImportError:
    from model import SasRec


# =======================
# CẤU HÌNH
# =======================
MAX_LEN = 50
NUM_NEG_TEST = 99   # 1 positive + 99 negative
SEED = 42

np.random.seed(SEED)
tf.random.set_seed(SEED)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

MODEL_PATH = os.path.join(PROJECT_ROOT, "data/model_registry/sasrec_v1.keras")
TEST_SET_PATH = os.path.join(PROJECT_ROOT, "data/model_registry/test_set.pkl")
ITEM_MAP_PATH = os.path.join(PROJECT_ROOT, "data/model_registry/item_map.json")


# =======================
# HÀM PAD SEQUENCE
# =======================
def pad_sequence(seq, max_len):
    # 🔥 ÉP KIỂU QUAN TRỌNG
    seq = list(seq)[-max_len:]

    pad_len = max_len - len(seq)

    return (
        np.array(seq + [0] * pad_len, dtype=np.int32),
        np.array([True] * len(seq) + [False] * pad_len, dtype=bool)
    )



# =======================
# MAIN EVALUATION
# =======================
def main():
    print("\n📊 ĐÁNH GIÁ SASRec (Sampled 1 vs 99 – TIMELINE SAFE)\n")

    # ---- Load model ----
    print("🔄 Load model...")
    model = tf.keras.models.load_model(MODEL_PATH, compile=False)

    # ---- Load test set ----
    print("🔄 Load test set...")
    with open(TEST_SET_PATH, "rb") as f:
        test_set = pickle.load(f)

    # ---- Load item map ----
    with open(ITEM_MAP_PATH, "r") as f:
        item_map = json.load(f)
    vocab_size = len(item_map)

    print(f"👥 Users test: {len(test_set)}")
    print(f"📦 Vocab size: {vocab_size}")

    hits_10 = 0
    ndcgs_10 = 0

    # ---- Evaluation loop ----
    for sample in tqdm(test_set, desc="Evaluating"):
        """
        sample = {
            'input_items': [...],   # history (KHÔNG gồm item cuối)
            'input_cats': [...],
            'label': item_id        # item cuối (ground truth)
        }
        """

        seq_items = sample["input_items"]
        seq_cats = sample["input_cats"]
        target_item = sample["label"]

        # =======================
        # NEGATIVE SAMPLING (TIMELINE SAFE)
        # =======================
        seen_items = set(seq_items)
        seen_items.add(target_item)

        test_items = [target_item]
        while len(test_items) < NUM_NEG_TEST + 1:
            neg = np.random.randint(1, vocab_size + 1)
            if neg not in seen_items:
                test_items.append(neg)

        # =======================
        # PAD INPUT
        # =======================
        in_items, mask = pad_sequence(seq_items, MAX_LEN)
        in_cats, _ = pad_sequence(seq_cats, MAX_LEN)

        in_items = np.expand_dims(in_items, axis=0)
        in_cats = np.expand_dims(in_cats, axis=0)
        mask = np.expand_dims(mask, axis=0)

        # =======================
        # MODEL FORWARD
        # =======================
        outputs = model(
            {
                "item_ids": in_items,
                "category_ids": in_cats,
                "padding_mask": mask
            },
            training=False
        )

        # Lấy embedding sequence
        seq_emb = outputs["item_sequence_embedding"][0]   # [L, D]

        # Embedding tại timestep cuối
        user_emb = seq_emb[-1]                      # [D]

        # =======================
        # TÍNH SCORE 100 ITEM
        # =======================
        item_weights = model.item_embedding.embeddings    # [V, D]
        test_items_idx = tf.constant(test_items, dtype=tf.int32)

        test_item_embs = tf.gather(item_weights, test_items_idx)  # [100, D]
        scores = tf.matmul(test_item_embs, tf.expand_dims(user_emb, -1))
        scores = tf.squeeze(scores).numpy()               # [100]

        # =======================
        # RANKING
        # =======================
        rank = np.sum(scores > scores[0])

        if rank < 10:
            hits_10 += 1
            ndcgs_10 += 1.0 / np.log2(rank + 2)

    # =======================
    # METRICS
    # =======================
    num_users = len(test_set)
    hr_10 = hits_10 / num_users
    ndcg_10 = ndcgs_10 / num_users

    print("\n" + "=" * 50)
    print("🏆 KẾT QUẢ ĐÁNH GIÁ SASRec")
    print(f"👥 Users         : {num_users}")
    print(f"🎯 Hit Rate @10  : {hr_10:.4f} ({hr_10 * 100:.2f}%)")
    print(f"⭐ NDCG @10      : {ndcg_10:.4f}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()

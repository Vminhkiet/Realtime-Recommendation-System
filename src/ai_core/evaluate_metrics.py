import os
import json
import pickle
import numpy as np
import tensorflow as tf
from tqdm import tqdm
import math
import re

try:
    from .model import SasRec
except ImportError:
    from model import SasRec


# ======================================================
# CONFIG
# ======================================================
MAX_LEN = 50
NUM_NEG_TEST = 99
TOP_K = 10
SEED = 42

np.random.seed(SEED)
tf.random.set_seed(SEED)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))

MODEL_PATH = os.path.join(PROJECT_ROOT, "data/model_registry/sasrec_v1.keras")
TEST_SET_PATH = os.path.join(PROJECT_ROOT, "data/model_registry/test_set.pkl")
ITEM_MAP_PATH = os.path.join(PROJECT_ROOT, "data/model_registry/item_map.json")


# ======================================================
# PAD SEQUENCE
# ======================================================
def pad_sequence(seq, max_len):
    seq = list(seq)[-max_len:]
    pad_len = max_len - len(seq)

    return (
        np.array(seq + [0] * pad_len, dtype=np.int32),
        np.array([True] * len(seq) + [False] * pad_len, dtype=bool)
    )


# ======================================================
# METRICS
# ======================================================
def precision_at_k(preds, target, k):
    return 1.0 / k if target in preds[:k] else 0.0


def recall_at_k(preds, target, k):
    return 1.0 if target in preds[:k] else 0.0


def hit_rate_at_k(preds, target, k):
    return 1 if target in preds[:k] else 0


def mrr_at_k(preds, target, k):
    for i, item in enumerate(preds[:k]):
        if item == target:
            return 1.0 / (i + 1)
    return 0.0


def ndcg_at_k(preds, target, k):
    for i, item in enumerate(preds[:k]):
        if item == target:
            return 1.0 / math.log2(i + 2)
    return 0.0


# ======================================================
# MAIN EVALUATION
# ======================================================
def main():
    print("\n📊 SASRec Evaluation (1 vs 99 – FULL METRICS)\n")

    # ---- Load model ----
    model = tf.keras.models.load_model(MODEL_PATH, compile=False)

    # ---- Load test set ----
    with open(TEST_SET_PATH, "rb") as f:
        test_set = pickle.load(f)

    # ---- Load item map ----
    with open(ITEM_MAP_PATH, "r") as f:
        item_map = json.load(f)

    vocab_size = len(item_map)
    print(f"👥 Test users: {len(test_set)}")
    print(f"📦 Items     : {vocab_size}")

    # ---- Metrics accumulators ----
    p_sum = r_sum = mrr_sum = ndcg_sum = 0.0
    hr_sum = 0

    # ==================================================
    # LOOP
    # ==================================================
    for sample in tqdm(test_set, desc="Evaluating"):
        seq_items = sample["input_items"]
        seq_cats = sample["input_cats"]
        target_item = sample["label"]

        # ---- Negative sampling (timeline safe) ----
        seen = set(seq_items)
        seen.add(target_item)

        test_items = [target_item]
        while len(test_items) < NUM_NEG_TEST + 1:
            neg = np.random.randint(1, vocab_size + 1)
            if neg not in seen:
                test_items.append(neg)

        # ---- Pad input ----
        in_items, mask = pad_sequence(seq_items, MAX_LEN)
        in_cats, _ = pad_sequence(seq_cats, MAX_LEN)

        in_items = in_items[None, :]
        in_cats = in_cats[None, :]
        mask = mask[None, :]

        # ---- Forward ----
        outputs = model(
            {
                "item_ids": in_items,
                "category_ids": in_cats,
                "padding_mask": mask
            },
            training=False
        )

        seq_emb = outputs["item_sequence_embedding"][0]
        user_emb = seq_emb[-1]

        # ---- Score 100 items ----
        item_weights = model.item_embedding.embeddings
        test_items_tf = tf.constant(test_items, dtype=tf.int32)
        test_embs = tf.gather(item_weights, test_items_tf)

        scores = tf.squeeze(
            tf.matmul(test_embs, tf.expand_dims(user_emb, -1))
        ).numpy()

        # ---- Ranking ----
        ranked_idx = np.argsort(-scores)
        ranked_items = [test_items[i] for i in ranked_idx]

        # ---- Metrics ----
        p_sum += precision_at_k(ranked_items, target_item, TOP_K)
        r_sum += recall_at_k(ranked_items, target_item, TOP_K)
        hr_sum += hit_rate_at_k(ranked_items, target_item, TOP_K)
        mrr_sum += mrr_at_k(ranked_items, target_item, TOP_K)
        ndcg_sum += ndcg_at_k(ranked_items, target_item, TOP_K)

    # ==================================================
    # RESULTS
    # ==================================================
    n = len(test_set)

    print("\n" + "=" * 60)
    print("🏆 SASRec Evaluation Results")
    print(f"Users        : {n}")
    print(f"Precision@10 : {p_sum / n:.4f}")
    print(f"Recall@10    : {r_sum / n:.4f}")
    print(f"HR@10        : {hr_sum / n:.4f}")
    print(f"MRR@10       : {mrr_sum / n:.4f}")
    print(f"NDCG@10      : {ndcg_sum / n:.4f}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()

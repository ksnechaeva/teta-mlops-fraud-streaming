"""Offline-only artifact builder. It is never called by docker-compose."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.metrics import precision_recall_curve, roc_auc_score

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scorer_service" / "src"))
from preprocessing import transform_transactions  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-csv", required=True)
    parser.add_argument("--output", default=ROOT / "scorer_service/model/fraud_model.joblib")
    parser.add_argument("--chunksize", type=int, default=50_000)
    args = parser.parse_args()

    model = SGDClassifier(loss="log_loss", alpha=2e-6, random_state=42, average=True)
    rng = np.random.default_rng(42)
    validation_x = []
    validation_y = []
    first = True

    for chunk_number, chunk in enumerate(pd.read_csv(args.train_csv, chunksize=args.chunksize)):
        labels = chunk.pop("target").astype(int).to_numpy()
        matrix = transform_transactions(chunk.to_dict(orient="records"))
        weights = np.where(labels == 1, 20.0, 1.0)
        model.partial_fit(matrix, labels, classes=np.array([0, 1]) if first else None, sample_weight=weights)
        first = False
        if len(validation_y) < 100_000:
            take = rng.choice(len(chunk), size=min(5_000, len(chunk)), replace=False)
            validation_x.append(matrix[take])
            validation_y.append(labels[take])
        print(f"trained chunk {chunk_number + 1}")

    from scipy.sparse import vstack

    x_val = vstack(validation_x)
    y_val = np.concatenate(validation_y)
    raw_scores = model.decision_function(x_val).reshape(-1, 1)
    calibrator = LogisticRegression(random_state=42).fit(raw_scores, y_val)
    probabilities = calibrator.predict_proba(raw_scores)[:, 1]
    precision, recall, thresholds = precision_recall_curve(y_val, probabilities)
    f2 = 5 * precision[:-1] * recall[:-1] / np.maximum(4 * precision[:-1] + recall[:-1], 1e-12)
    threshold = float(thresholds[int(np.nanargmax(f2))]) if len(thresholds) else 0.5
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "calibrator": calibrator, "threshold": threshold}, output)
    print(f"saved={output} threshold={threshold:.6f} validation_auc={roc_auc_score(y_val, probabilities):.6f}")


if __name__ == "__main__":
    main()

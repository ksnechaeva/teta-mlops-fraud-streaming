"""CPU-only model loading and fraud scoring."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import joblib

from .preprocessing import transform_transactions

DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[1] / "model" / "fraud_model.joblib"


class FraudScorer:
    def __init__(self, model_path: str | Path | None = None, threshold: float | None = None):
        artifact_path = Path(model_path or os.getenv("MODEL_PATH", DEFAULT_MODEL_PATH))
        artifact = joblib.load(artifact_path)
        self.model = artifact["model"]
        self.calibrator = artifact.get("calibrator")
        self.threshold = float(threshold or os.getenv("FRAUD_THRESHOLD", artifact["threshold"]))

    def predict(self, transaction: dict[str, Any]) -> tuple[float, int]:
        matrix = transform_transactions([transaction])
        if self.calibrator is None:
            score = float(self.model.predict_proba(matrix)[0, 1])
        else:
            raw_score = self.model.decision_function(matrix).reshape(-1, 1)
            score = float(self.calibrator.predict_proba(raw_score)[0, 1])
        return score, int(score >= self.threshold)

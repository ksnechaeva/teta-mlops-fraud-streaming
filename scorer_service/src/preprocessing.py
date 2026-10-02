"""Pure preprocessing used by both offline training and online inference."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

import pandas as pd
from sklearn.feature_extraction import FeatureHasher

HASH_FEATURES = 2**16
CATEGORICAL_COLUMNS = (
    "merch",
    "cat_id",
    "gender",
    "one_city",
    "us_state",
    "jobs",
)
NUMERIC_COLUMNS = (
    "amount",
    "population_city",
    "lat",
    "lon",
    "merchant_lat",
    "merchant_lon",
)
REQUIRED_COLUMNS = {"transaction_time", *CATEGORICAL_COLUMNS, *NUMERIC_COLUMNS}


def _safe_float(value: Any) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def validate_transaction(transaction: dict[str, Any]) -> None:
    missing = sorted(REQUIRED_COLUMNS.difference(transaction))
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")


def transaction_to_features(transaction: dict[str, Any]) -> dict[str, float]:
    """Turn one raw test.csv row into bounded numerical/hash features."""
    validate_transaction(transaction)
    try:
        timestamp = datetime.fromisoformat(str(transaction["transaction_time"]))
    except (TypeError, ValueError) as error:
        raise ValueError("transaction_time must be a valid datetime") from error

    lat = _safe_float(transaction["lat"])
    lon = _safe_float(transaction["lon"])
    merchant_lat = _safe_float(transaction["merchant_lat"])
    merchant_lon = _safe_float(transaction["merchant_lon"])
    amount = max(_safe_float(transaction["amount"]), 0.0)
    population = max(_safe_float(transaction["population_city"]), 0.0)

    features: dict[str, float] = {
        "num:log_amount": math.log1p(amount) / 12.0,
        "num:log_population": math.log1p(population) / 16.0,
        "num:lat_delta": min(abs(lat - merchant_lat), 180.0) / 180.0,
        "num:lon_delta": min(abs(lon - merchant_lon), 360.0) / 360.0,
        "num:hour_sin": math.sin(2 * math.pi * timestamp.hour / 24),
        "num:hour_cos": math.cos(2 * math.pi * timestamp.hour / 24),
        "num:dow_sin": math.sin(2 * math.pi * timestamp.weekday() / 7),
        "num:dow_cos": math.cos(2 * math.pi * timestamp.weekday() / 7),
    }
    for column in CATEGORICAL_COLUMNS:
        value = transaction.get(column)
        token = "<NA>" if pd.isna(value) else str(value).strip().lower()
        features[f"cat:{column}={token}"] = 1.0
    return features


def transform_transactions(transactions: list[dict[str, Any]]):
    if not transactions:
        raise ValueError("At least one transaction is required")
    rows = [transaction_to_features(row) for row in transactions]
    return FeatureHasher(
        n_features=HASH_FEATURES,
        input_type="dict",
        alternate_sign=False,
    ).transform(rows)

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scorer_service"))
sys.path.insert(0, str(ROOT / "scorer_service" / "src"))

from app import score_payload
from preprocessing import HASH_FEATURES, transform_transactions


TRANSACTION = {
    "transaction_time": "2019-09-14 02:46",
    "merch": "fraud_Stokes, Christiansen and Sipes",
    "cat_id": "grocery_net",
    "amount": 25.79,
    "name_1": "Michael",
    "name_2": "Rodriguez",
    "gender": "M",
    "street": "172 Paula Inlet Apt. 650",
    "one_city": "Cross Plains",
    "us_state": "TX",
    "post_code": 76443,
    "lat": 32.1482,
    "lon": -99.1872,
    "population_city": 1897,
    "jobs": "Chief Operating Officer",
    "merchant_lat": 31.772057,
    "merchant_lon": -99.103183,
}


def test_preprocessing_shape_and_finiteness():
    matrix = transform_transactions([TRANSACTION])
    assert matrix.shape == (1, HASH_FEATURES)
    assert np.isfinite(matrix.data).all()


def test_score_contract():
    class StubScorer:
        def predict(self, transaction):
            assert transaction == TRANSACTION
            return 0.91, 1

    result = score_payload({"transaction_id": "tx-1", "data": TRANSACTION}, StubScorer())
    assert result == {"transaction_id": "tx-1", "score": 0.91, "fraud_flag": 1}

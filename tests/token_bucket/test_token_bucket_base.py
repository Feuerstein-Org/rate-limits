"""Contains tests for the TokenBucketConfig initialization and type validation."""

from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from steindamm.token_bucket.token_bucket_base import TokenBucketBase


@pytest.mark.parametrize(
    "config_params,error",
    [
        ({"name": "test"}, None),
        ({"name": None}, ValidationError),
        ({"name": 1}, ValidationError),
        ({"name": True}, ValidationError),
        ({"capacity": 2}, None),
        ({"capacity": 2.2}, None),
        ({"capacity": -1}, ValidationError),
        ({"capacity": None}, ValidationError),
        ({"capacity": "test"}, ValidationError),
        ({"refill_frequency": 2}, None),
        ({"refill_frequency": 2.2}, None),
        ({"refill_frequency": "test"}, ValidationError),
        ({"refill_frequency": None}, ValidationError),
        ({"refill_frequency": -1}, ValidationError),
        ({"refill_amount": 1}, None),
        ({"refill_amount": 0.8}, None),
        ({"refill_amount": -1}, ValidationError),
        ({"refill_amount": "test"}, ValidationError),
        ({"refill_amount": None}, ValidationError),
        ({"initial_tokens": 1}, None),
        ({"initial_tokens": 0.5}, None),
        ({"initial_tokens": 0}, None),
        ({"initial_tokens": -1}, ValidationError),
        ({"initial_tokens": "test"}, ValidationError),
        ({"tokens_to_consume": 0.5}, None),
        ({"tokens_to_consume": 0}, None),
        ({"tokens_to_consume": -1}, ValidationError),
        ({"tokens_to_consume": "test"}, ValidationError),
        ({"tokens_to_consume": None}, ValidationError),
        ({"initial_tokens": 1, "capacity": 2}, None),
        ({"refill_amount": 3, "capacity": 2}, ValidationError),
        ({"initial_tokens": 3, "capacity": 2}, ValidationError),
        ({"tokens_to_consume": 3, "capacity": 4}, None),
        ({"tokens_to_consume": 3, "capacity": 2}, ValidationError),
        ({"max_sleep": 20}, None),
        ({"max_sleep": 0}, None),
        ({"max_sleep": "test"}, ValidationError),
        ({"max_sleep": None}, ValidationError),
        ({"expiry": 60}, None),
        ({"expiry": 0}, ValidationError),
        ({"expiry": -1}, ValidationError),
        ({"expiry": 1.5}, ValidationError),
        ({"expiry": "test"}, ValidationError),
        ({"expiry": None}, ValidationError),
        ({"window_start_time": datetime(2020, 1, 1)}, None),  # Past datetime is valid
        ({"window_start_time": datetime(2099, 1, 1)}, ValidationError),  # Past datetime is invalid
        ({"window_start_time": "not_a_datetime"}, ValidationError),
        ({"refill_amount": 0, "refill_frequency": 0}, None),  # Valid non-refilling bucket
        ({"refill_amount": 0.0, "refill_frequency": 0.0}, None),
        ({"refill_amount": 0, "refill_frequency": 1}, ValidationError),  # Invalid: only refill_amount is 0
        ({"refill_amount": 1, "refill_frequency": 0}, ValidationError),  # Invalid: only refill_frequency is 0
    ],
)
def test_init_types(config_params: dict[str, Any], error: type[ValidationError] | None) -> None:
    """Test TokenBucketConfig initialization with various parameter types and values."""
    if "name" not in config_params:
        config_params["name"] = "test"
    if error:
        with pytest.raises(error):
            TokenBucketBase(**config_params)

    else:
        TokenBucketBase(**config_params)

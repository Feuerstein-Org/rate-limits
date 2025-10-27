"""Contains tests for the TokenBucketConfig initialization and type validation."""

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
        ({"tokens_to_consume": 0.5}, None),
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

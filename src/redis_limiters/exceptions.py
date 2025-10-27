"""Exceptions for redis_limiters package."""


class MaxSleepExceededError(Exception):
    """Raised when we've slept for longer than the `max_sleep` specified limit."""

    pass

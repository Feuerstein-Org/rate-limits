"""Exceptions for steindamm package."""


class MaxSleepExceededError(Exception):
    """Raised when we've slept for longer than the `max_sleep` specified limit."""

    pass


class NoTokensAvailableError(Exception):
    """Raised when a non-refilling bucket runs out of tokens."""

    pass

"""
Unit tests for the redis-limiters package.

These tests require a Redis server running locally on port 6379 for standalone Redis tests
and a Redis Cluster running locally on port 6380 for cluster tests.

use `mise run test-setup` to start the Redis servers in Docker containers.

They can be stopped with `mise run test-teardown`.

To run the tests, use: `mise run test`, the whole test suite can also be run with `mise run test-full.
"""

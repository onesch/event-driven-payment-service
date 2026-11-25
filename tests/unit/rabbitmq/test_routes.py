import pytest

from src.rabbitmq.routes import EVENT_ROUTES, EventRoute, RetryPolicy


# Test cases for RetryPolicy


def test_retry_policy_defaults_to_three_attempts():
    policy = RetryPolicy()

    assert policy.max_attempts == 3
    assert policy.max_retries == 2


@pytest.mark.parametrize(
    ("retry", "expected_delay_ms"),
    [(1, 5_000), (2, 10_000), (3, 20_000)],
)
def test_retry_policy_delay_grows_exponentially(retry, expected_delay_ms):
    assert RetryPolicy().delay_ms(retry) == expected_delay_ms


def test_retry_policy_supports_custom_backoff():
    policy = RetryPolicy(max_attempts=4, base_delay_ms=100, backoff_factor=3)

    assert policy.max_retries == 3
    assert [policy.delay_ms(retry) for retry in (1, 2, 3)] == [100, 300, 900]


# Test cases for EventRoute


def test_event_route_derives_dead_letter_and_retry_names():
    route = EventRoute(
        exchange="payments",
        routing_key="payments.created",
        queue="payments.new",
    )

    assert route.dead_letter_exchange == "payments.dlx"
    assert route.dead_letter_queue == "payments.new.dlq"
    assert route.retry_queue(1) == "payments.new.retry.1"
    assert route.retry == RetryPolicy()


def test_event_routes_are_hashable():
    """declare_topology() deduplicates routes through a set."""
    assert len(set(EVENT_ROUTES.values())) == len(EVENT_ROUTES)


def test_payment_created_route():
    route = EVENT_ROUTES["payment.created"]

    assert route.exchange == "payments"
    assert route.routing_key == "payments.created"
    assert route.queue == "payments.new"
    assert route.retry.max_attempts == 3

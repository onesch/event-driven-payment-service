"""
Mapping of event types to their RabbitMQ routes.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times an event is processed and how long to wait in between."""

    # Total processing attempts, including the first delivery.
    max_attempts: int = 3
    # Delay before the first retry; each next retry waits `backoff_factor` times longer.
    base_delay_ms: int = 5_000  # 5 seconds
    backoff_factor: int = 2

    @property
    def max_retries(self) -> int:
        return self.max_attempts - 1

    def delay_ms(self, retry: int) -> int:
        """Delay before the given retry (1-based)."""
        return self.base_delay_ms * self.backoff_factor ** (retry - 1)


@dataclass(frozen=True, slots=True)
class EventRoute:
    exchange: str
    routing_key: str
    queue: str
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    @property
    def dead_letter_exchange(self) -> str:
        return f"{self.exchange}.dlx"

    @property
    def dead_letter_queue(self) -> str:
        return f"{self.queue}.dlq"

    def retry_queue(self, retry: int) -> str:
        """Name of the queue that holds messages waiting for the given retry."""
        return f"{self.queue}.retry.{retry}"


# Define the mapping of event types to their corresponding routes.
EVENT_ROUTES: dict[str, EventRoute] = {
    "payment.created": EventRoute(
        exchange="payments",
        routing_key="payments.created",
        queue="payments.new",
    ),
}

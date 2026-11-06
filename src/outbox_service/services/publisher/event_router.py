from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EventRoute:
    exchange: str
    routing_key: str
    queue: str


# Define the mapping of event types to their corresponding routes.
EVENT_ROUTES: dict[str, EventRoute] = {
    "payment.created": EventRoute(
        exchange="payments",
        routing_key="payments.created",
        queue="payments.new"
    ),
}

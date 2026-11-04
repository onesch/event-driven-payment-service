from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EventRoute:
    exchange: str
    routing_key: str
    queue: str


EVENT_ROUTES: dict[str, EventRoute] = {
    "payment.created": EventRoute(exchange="payments", routing_key="payments.created", queue="payments.new"),
}

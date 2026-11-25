from faststream import FastStream

from src.payment_worker.consumers.payment_consumer import broker
from src.rabbitmq.topology import declare_topology

app = FastStream(broker)


@app.on_startup
async def setup_topology() -> None:
    """
    Declare the full topology before subscribers start consuming.

    The subscriber itself only declares its own queue; retry queues and the
    DLQ must exist before the first failed message is published to them,
    otherwise RabbitMQ silently drops it as unroutable.
    """
    await broker.connect()
    await declare_topology(broker)

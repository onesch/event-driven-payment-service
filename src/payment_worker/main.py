from faststream import FastStream

from src.payment_worker.consumers.payment_consumer import broker

app = FastStream(broker)

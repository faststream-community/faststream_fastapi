# Connection

## Startup setup
You can configure the importance of connecting to brokers using the `safe_connection` parameter.

If `safe_connection=True`, the application will not start if the connection to the broker fails.

If `safe_connection=False`, the application will start even if there is a connection error, followed by an error in the logs.

!!! tip
    By default `safe_connection` is `False`

## Connection hook

You can also set up your own hook for the connection.

Example with tenacity:
```python
import uvicorn
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from faststream_fastapi import FastStreamAPI
from tenacity import retry

@retry
async def my_connection_hook(broker: KafkaBroker) -> None:
    await broker.start()

application = FastStreamAPI(
    KafkaBroker(),
    application=FastAPI(),
    connection_hook=my_connection_hook,
)
uvicorn.run(application)
```

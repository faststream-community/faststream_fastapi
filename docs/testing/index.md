# Testing

Testing with the plugin is very simple.

All you need to do is wrap your broker in TestBroker and then transfer the broker to FastStreamAPI.

Example:

```python
import asyncio

from fastapi.testclient import TestClient
from faststream.nats import NatsBroker, TestNatsBroker
from faststream_fastapi.faststream_api import FastAPI, FastStreamAPI

fastapi = FastAPI()
broker = NatsBroker()
application = FastStreamAPI(broker, application=fastapi)

@broker.subscriber("root")
async def handle1(msg: str) -> str:
    return "handle"

@fastapi.get("/")
async def handle() -> str:
    response = await broker.request("", "root")
    return await response.decode() or ""

async def main():
    async with TestNatsBroker(broker):
        with TestClient(application) as client:
            r = client.get("/")
            print(r.json())

asyncio.run(main())
```

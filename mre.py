from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, Request
from faststream.nats import NatsBroker

from faststream_fastapi.faststream_api import FastStreamApi

broker = NatsBroker()

@broker.subscriber("broker")
async def handler1(
    request: Request,
    dep: int = Depends(lambda: 1)
):
    print("BROKER", dep, request.state.data)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.publish(None, "broker")
    yield {"data": "LIFESPAN DATA"}


fastapi_app = FastAPI(lifespan=lifespan)
app = FastStreamApi(fastapi_app, [broker])
uvicorn.run(app)

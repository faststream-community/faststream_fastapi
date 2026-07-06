from contextlib import asynccontextmanager
from typing import Annotated

from pydantic import BaseModel
import uvicorn
from fastapi import Body, Depends, FastAPI, Header, Path, Request
# from faststream import Path
from faststream.nats import NatsBroker

from faststream_fastapi.faststream_api import FastStreamApi

broker = NatsBroker()

class BodyModel(BaseModel):
    field: int

class Dep: ...

@broker.subscriber("subject.{num}")
async def handler1(
    request: Request,
    num: Annotated[int, Depends(Dep)],
    body: Annotated[BodyModel, Body()],
    x_user_id: Annotated[int, Header()],
) -> None:
    print(num)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.publish({"field": 12}, "subject.2", headers={"x-user-id": "123456"},)
    yield {"data": "LIFESPAN DATA"}


fastapi_app = FastAPI(lifespan=lifespan)
app = FastStreamApi(fastapi_app, [broker])
uvicorn.run(app)

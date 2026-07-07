from contextlib import asynccontextmanager
from typing import Annotated

from pydantic import BaseModel
from fastapi import APIRouter, Body, Depends, FastAPI, Header, Request, BackgroundTasks
from faststream.nats import NatsBroker, NatsRouter
import uvicorn

from faststream_fastapi import Context, AsyncAPIRouter, FastStreamApi


class BodyModel(BaseModel):
    field: int


class Dep: ...

sub_router = NatsRouter()
@sub_router.subscriber("subject.{num}")
async def handler1(
    request: Request,
    tasks: BackgroundTasks,
    num: Annotated[int, Depends(lambda: Dep())],
    body: Annotated[BodyModel, Body()],
    x_user_id: Annotated[int, Header()],
    fastapi_app: Annotated[FastAPI, Context("fastapi_app")]
) -> None:
    print(23)
    print(num, tasks)
    print(23)
    


api_router = APIRouter()
@api_router.get("/")
async def handler() -> None:
    ...


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.publish({"field": 12}, "subject.2", headers={"x-user-id": "123456"},)
    yield {"data": "LIFESPAN DATA"}


fastapi_app = FastAPI(lifespan=lifespan)
fastapi_app.openapi
fastapi_app.include_router(api_router)

broker = NatsBroker()
broker.include_router(sub_router)

app = FastStreamApi(
    fastapi_app,
    [broker],
    asyncapi_path=AsyncAPIRouter(
        "/fs_docs",
        include_in_schema=True,
    )
)
uvicorn.run(app)

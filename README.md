# Плагин fastapi для faststream 

## API Подписчиков
```py
class BodyModel(BaseModel):
    field: int

@broker.subscriber("subject.{num}")
async def subscriber(
    request: Request,
    num: Annotated[int, Path()],
    body: Annotated[BodyModel, Body()],
    x_user_id: Annotated[int, Header()],
    my_dep: Annotated[int, Depends(int)],
    fastapi_app: Annotated[FastAPI, Context("fastapi_app")],
) -> None:
    ...
```
В подписчиках можно использовать `fastapi.Request`, `fastapi.Path`, `fastapi.Body`, `fastapi.Header`, `fastapi.Depends` и `faststream_fastapi.Context`

## Lifespan
1. На момент запуска lifespan брокеры будут запущены.

2. Доступ из lifespan осуществляется с помощью обычного механизма. FastAPI `request.state` 
```py
@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.publish(None, "subject")
    yield {"lifespan_data": "LIFESPAN DATA"}

@broker.subscriber("subject")
async def subscriber(request: Request):
    print(request.state.lifespan_data)
```

3. Брокеры останавливаются после заверщения выполнения lifespan.

## Dependency overrides
```py
fastapi = FastAPI()
fastapi.dependency_overrides[Dep] = lambda: "Dep"

app = FastStreamAPI(
    fastapi,
    [NatsBroker()],
)
```

## AsyncAPI
Для настройки AsyncAPI можно использовать `SpecificationsFactory` из самого faststream, а так же `faststream_fastapi.AsyncAPIRouter`
```py
FastStreamAPI(
    ...,
    specification=AsyncAPI(
        title="My app",
        version="1.0.0",
        description="...",
        ...,
    ),
    asyncapi_path="/fs_docs",
    # or
    asyncapi_path=AsyncAPIRouter(
        "/fs_docs",
        description="...",
        ...
    ),
)
```

## Background tasks

```py
@broker.subscriber("subject")
async def handler1(
    tasks: BackgroundTasks,
) -> None:
    tasks.add_task(...)
```
Таски выполняются после исполнения хендлера в мидлевари


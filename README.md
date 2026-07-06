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
    my_dep: Annotated[int, Depends(int)]
) -> None:
    ...
```
В подписчиках можно использовать `fastapi.Request`, `fastapi.Path`, `fastapi.Body`, `fastapi.Header` и `fastapi.Depends`.

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
class DependencyOverridesProvider:
    dependency_overrides = {
        Dep: lambda: "Dep",
    }

app = FastStreamApi(
    FastAPI(),
    [NatsBroker()],
    dependency_overrides_provider=DependencyOverridesProvider,
)
```

## AsyncAPI
Настройка AsyncAPI происходит следующим образом
```py
FastStreamApi(
    ...,
    include_in_schema=True,
    schema_url="...",
    specification=AsyncAPI(
        title="My app",
        version="1.0.0",
        description="...",
        ...,
    )
)

Все фишки из FastStream AsyncAPI доступны и в плагине

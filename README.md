# FastAPI Plugin for FastStream

A plugin that allows you to use **Depends** and **FastAPI** other objects in the **FastStream**

# Features

### Use FastAPI Dependency Injection
In **FastStream** handlers, it will be possible to use the familiar DI from **FastAPI**
```py
from fastapi import Path, Body, Header, Depends

class BodyModel(BaseModel):
    field: int

@broker.subscriber("subject.{num}")
async def subscriber_handler(
    num: Annotated[int, Path()],
    body: Annotated[BodyModel, Body()],
    x_user_id: Annotated[int, Header()],
    my_dep: Annotated[int, Depends(int)],
) -> None:
    ...
```

### Using the FastStream Context
```py
from faststream_fastapi import Context

@broker.subscriber("subject")
async def subscriber_handler(context_data: Annotated[int, Context("data")]) -> Response: ...
```

### Use FastAPI Request and Response
```py
from fastapi import Request, Response
from fastapi.responses import JSONResponse

@broker.subscriber("subject")
async def subscriber_handler(request: Request) -> Response:
    return JSONResponse({"data": 1})
```

### Minimalistic plugin connection to your application

All you need to do is wrap the **FastAPI** with the **FastStreamAPI** object from the **plugin**

```py
fastapi = FastAPI()

application = FastStreamAPI(
    NatsBroker(),
    application=fastapi,
)
uvicorn.run(application)
```

### Managing a lifespan state
Now the **lifespan state** is also available in **FastStream handlers**

Which was not the case in the old plugin

```py
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield {"lifespan_data": "LIFESPAN DATA"}

@broker.subscriber("subject")
async def subscriber(request: Request) -> None:
    assert request.state.lifespan_data == "LIFESPAN DATA"
```

### Сan use FastAPI dependency overrides
```py
fastapi = FastAPI()
fastapi.dependency_overrides[Dep] = lambda: "Dep"

@broker.subscriber("subject")
async def subscriber(dep: Annotated[str, De[]]) -> None:
    assert dep == "Dep"
```

### The ability to configure AsyncAPI

The ability to configure **AsyncAPI** using **SpecificationFactory** from **FastStream** and **AsyncAPIConfig** from the **plugin**

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

### Backgrounds tasks

You can use **BackgroundTasks** from **FastAPI** in **FastStream handlers**

```py
@broker.subscriber("subject")
async def handler1(
    tasks: BackgroundTasks,
) -> None:
    tasks.add_task(...)
```

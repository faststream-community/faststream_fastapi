# Dependencies

**faststream_fastapi** uses **FastAPI** for dependency management.

!!! Tip
    This means that you cannot use DI FastStream with the plugin, only DI FastAPI.

## Сan be used from fastAPI?

You can use:
Request
Response
Path
Body
Header


## Using Annotated

Dependencies also can be used with Annotated

```py
from fastapi import Depends
from faststream_fastapi import Logger

async def base_dep(user_id: int) -> bool:
    return True

@broker.subscriber("in-test")
async def base_handler(
    user: str,
    logger: Logger,
    dep: bool = Depends(base_dep),
) -> None:
    assert dep is True
    logger.info(user)
```

## Dependency Injection

To implement dependencies in **faststream_fastapi**, a special class called **Depends** is used

```py
from fastapi import Depends

def simple_dependency() -> int:
    return 1

@broker.subscriber("test")
async def handler(body: dict, d: int = Depends(simple_dependency)) -> None:
    assert d == 1
```

## Top-level Dependencies

If you don't need a dependency result, you can use the following code:

```py
@broker.subscriber("test")
def method(_ = Depends(...)) -> None: ...
```

But, using a special subscriber parameter is much more suitable:

```py
@broker.subscriber("test", dependencies=[Depends(...)])
def method() -> None: ...
```

You can also declare broker-level dependencies, which will be applied to all broker's handlers:

```py
broker = NatsBroker(dependencies=[Depends(...)])
```

## Nested Dependencies

Dependencies can also contain other dependencies. This works in a very predictable way: just declare Depends in the dependent function.

```py
from fastapi import Depends

def another_dependency() -> int:
    return 1

def simple_dependency(b: int = Depends(another_dependency)) -> int:
    return b * 2

@broker.subscriber("test")
async def handler(
    body: dict,
    a: int = Depends(another_dependency),
    b: int = Depends(simple_dependency),
):
    assert a + b == 3
```

## Dependency overrides

To do this, you need to use the dependency overrides mechanism from FastAPI itself.

```py
from fastapi import FastAPI
from faststream_fastapi import FastStreamAPI

def dependency() -> int:
    return 1

def mock_dependency() -> int:
    return 2

application = FastAPI()
application.dependency_overrides[dependency] = mock_dependency

FastStreamAPI(broker, application=application)
```

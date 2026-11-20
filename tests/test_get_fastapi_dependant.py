from typing import Annotated

from fastapi import Depends

from faststream_fastapi._internal.get_dependant import get_fastapi_dependant


def test_get_fastapi_dependant_has_faststream_fields() -> None:
    async def dependency() -> str:
        return "dependency"

    async def handler(
        message: str,
        dependency_value: Annotated[str, Depends(dependency)],
    ) -> None:
        pass

    dependant = get_fastapi_dependant(handler, ())

    assert dependant.call is handler
    assert len(dependant.dependencies) == 1
    assert dependant.dependencies[0].call is dependency
    assert dependant.model.__name__ == "handler"
    assert dependant.custom_fields == {}
    assert [field.field_name for field in dependant.flat_params] == ["message"]


def test_nested_dependency_fields() -> None:
    def nested(value: int) -> int:
        return value

    def dependency(value: int = Depends(nested)) -> int:
        return value

    async def handler(value: int = Depends(dependency)) -> int:
        return value

    dependant = get_fastapi_dependant(handler, ())
    assert [field.field_name for field in dependant.flat_params] == ["value"]

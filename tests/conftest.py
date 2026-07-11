import asyncio
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from faststream_fastapi._internal.fs_re_exports.context import ContextRepo


@pytest.hookimpl(tryfirst=True)
def pytest_keyboard_interrupt(
    excinfo: pytest.ExceptionInfo[KeyboardInterrupt],
) -> None:
    pytest.mark.skip("Interrupted Test Session")  # pragma: no cover


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        item.add_marker("all")


@pytest.fixture()
def queue() -> str:
    return str(uuid4())


@pytest.fixture()
def event() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture()
def event2() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture()
def mock() -> Generator[MagicMock, Any, None]:
    m = MagicMock()
    yield m
    m.reset_mock()


@pytest.fixture()
def context() -> ContextRepo:
    return ContextRepo()

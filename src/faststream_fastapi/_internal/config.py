from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI
from fastapi.datastructures import State


class ConfigAsgiStateError(Exception):
    def __str__(self) -> str:
        return "ASGI state already is set"


@dataclass
class Config:
    application: FastAPI
    dependency_overrides_provider: Any | None
    asgi_state: State | None = None

    def set_asgi_state(self, asgi_state: State) -> None:
        if self.asgi_state is not None:
            raise ConfigAsgiStateError

        self.asgi_state = asgi_state

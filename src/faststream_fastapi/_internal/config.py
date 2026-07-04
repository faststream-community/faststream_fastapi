from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI
from fastapi.datastructures import State


@dataclass
class Config:
    application: FastAPI
    asgi_state: State | None = None
    dependency_overrides_provider: Any | None = None

    def set_asgi_state(self, asgi_state: State) -> None:
        self.asgi_state = asgi_state

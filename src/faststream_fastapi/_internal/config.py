from dataclasses import dataclass, field
from threading import RLock
from typing import Any

from fastapi import FastAPI

from faststream_fastapi._internal.fs_re_exports.context import ContextRepo


@dataclass
class Config:
    application: FastAPI
    dependency_overrides_provider: Any | None
    context: ContextRepo
    max_streaming_reply_size: int
    context_init_lock: RLock = field(default_factory=RLock)
    asgi_state: dict[str, Any] = field(default_factory=dict)

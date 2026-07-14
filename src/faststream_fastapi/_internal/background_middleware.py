from types import TracebackType
from typing import cast

from fastapi.background import BackgroundTasks
from faststream.middlewares import BaseMiddleware


class _BackgroundMiddleware(BaseMiddleware):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool | None:
        background = cast(
            "BackgroundTasks | None",
            getattr(self.context.get_local("message"), "background", None),
        )
        if exc_type is None and background is not None:
            await background()

        return await super().after_processed(exc_type, exc_val, exc_tb)

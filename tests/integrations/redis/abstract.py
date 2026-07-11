from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from faststream.redis import RedisBroker, TestRedisBroker
from typing_extensions import override

from tests.base.abstract import AbstractTestCaseConfig


class RedisAbstractTestCaseConfig(AbstractTestCaseConfig[RedisBroker]):
    @override
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RedisBroker:
        return RedisBroker(apply_types=apply_types, **kwargs)


class RedisAbstractInMemoryTestCaseConfig(RedisAbstractTestCaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: RedisBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[RedisBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: RedisBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[RedisBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: RedisBroker,
        **kwargs: Any,
    ) -> Any:
        return TestRedisBroker(*brokers, **kwargs)

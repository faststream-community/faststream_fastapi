from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from faststream.rabbit import RabbitBroker, TestRabbitBroker
from typing_extensions import override

from tests.base.abstract import AbstractTestCaseConfig


class RabbitAbstractTestCaseConfig(AbstractTestCaseConfig[RabbitBroker]):
    @override
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RabbitBroker:
        return RabbitBroker(apply_types=apply_types, **kwargs)


class RabbitAbstractInMemoryTestCaseConfig(RabbitAbstractTestCaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: RabbitBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[RabbitBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: RabbitBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[RabbitBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: RabbitBroker,
        **kwargs: Any,
    ) -> Any:
        return TestRabbitBroker(*brokers, **kwargs)

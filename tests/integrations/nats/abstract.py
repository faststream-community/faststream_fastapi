from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from faststream.nats import NatsBroker, TestNatsBroker
from typing_extensions import override

from tests.base.abstract import AbstractTestCaseConfig


class NatsAbstractTestCaseConfig(AbstractTestCaseConfig[NatsBroker]):
    @override
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> NatsBroker:
        return NatsBroker(apply_types=apply_types, **kwargs)


class NatsAbstractInMemoryTestCaseConfig(NatsAbstractTestCaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: NatsBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[NatsBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: NatsBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[NatsBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: NatsBroker,
        **kwargs: Any,
    ) -> Any:
        return TestNatsBroker(*brokers, **kwargs)

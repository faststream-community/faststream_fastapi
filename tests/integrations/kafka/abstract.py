from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from faststream.kafka import KafkaBroker, TestKafkaBroker
from typing_extensions import override

from tests.base.abstract import AbstractTestCaseConfig


class KafkaAbstractTestCaseConfig(AbstractTestCaseConfig[KafkaBroker]):
    @override
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> KafkaBroker:
        return KafkaBroker(apply_types=apply_types, **kwargs)


class KafkaAbstractInMemoryTestCaseConfig(KafkaAbstractTestCaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: KafkaBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[KafkaBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: KafkaBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[KafkaBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: KafkaBroker,
        **kwargs: Any,
    ) -> Any:
        return TestKafkaBroker(*brokers, **kwargs)

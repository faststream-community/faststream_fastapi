from contextlib import AbstractAsyncContextManager
from typing import Any, ClassVar, overload

from faststream.confluent import (
    KafkaBroker,
    TestKafkaBroker,
    TopicPartition,
)
from typing_extensions import override

from tests.base.abstract import AbstractTestCaseConfig


class ConfluentAbstractTestCaseConfig(AbstractTestCaseConfig[KafkaBroker]):
    timeout: ClassVar[float] = 10.0

    @override
    def get_subscriber_params(
        self,
        *topics: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        if len(topics) == 1:
            partitions = [TopicPartition(topics[0], partition=0, offset=0)]
            topics = ()
        else:
            partitions = []  # pragma: no cover

        return topics, {
            "auto_offset_reset": "earliest",
            "partitions": partitions,
            **kwargs,
        }

    @override
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> KafkaBroker:
        return KafkaBroker(apply_types=apply_types, **kwargs)


class ConfluentAbstractInMemoryTestCaseConfig(ConfluentAbstractTestCaseConfig):
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

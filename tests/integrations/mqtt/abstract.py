from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from faststream.mqtt.broker.broker import MQTTBroker
from faststream.mqtt.testing import TestMQTTBroker
from typing_extensions import override

from tests.base.abstract import AbstractTestCaseConfig


class MQTTAbstractTestCaseConfig(AbstractTestCaseConfig[MQTTBroker]):
    @override
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> MQTTBroker:
        return MQTTBroker(apply_types=apply_types, version="5.0", **kwargs)


class MQTTAbstractInMemoryTestCaseConfig(MQTTAbstractTestCaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: MQTTBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[MQTTBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: MQTTBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[MQTTBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: MQTTBroker,
        **kwargs: Any,
    ) -> Any:
        return TestMQTTBroker(*brokers, **kwargs)

import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def clean_prometheus_registry():
    """
    Cleans up the Prometheus REGISTRY before _and_ after every test.
    """
    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        REGISTRY.unregister(collector)

    yield

    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        REGISTRY.unregister(collector)

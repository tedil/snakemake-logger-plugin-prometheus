import logging
import pytest
from unittest.mock import patch
from snakemake_interface_logger_plugins.common import LogEvent
from snakemake_logger_plugin_prometheus import LogHandler, LogHandlerSettings


# Mock Snakemake resources
class MockResources:
    def __init__(self, **kwargs):
        self._names = list(kwargs.keys())
        for k, v in kwargs.items():
            setattr(self, k, v)

    # Simulate Snakemake's internal iterable behavior
    def __iter__(self):
        for name in self._names:
            yield getattr(self, name)


class MockOutputSettings:
    def __init__(self):
        self.dryrun = False


@pytest.fixture
def handler(clean_prometheus_registry):
    with patch("snakemake_logger_plugin_prometheus.start_http_server"):
        h = LogHandler(MockOutputSettings(), LogHandlerSettings(port=9091))
        yield h
        h.close()


def test_out_of_order_events(handler):
    """
    Test scenario where JOB_STARTED arrives *before* JOB_INFO.
    The start should be deferred and processed once INFO arrives.
    """
    job_id = 500
    rule = "async_rule"

    # 1. STARTED arrives first (Unknown ID)
    rec_start = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_start.event = LogEvent.JOB_STARTED
    rec_start.jobs = [job_id]  # Simulating legacy/mixed attribute
    handler.emit(rec_start)

    # Assertions: Should NOT be running yet (we don't know the rule name)
    # But it should be tracked internally as deferred
    assert handler.metric_running.labels(rule=rule)._value.get() == 0
    assert job_id in handler.deferred_starts

    # 2. INFO arrives later
    rec_info = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_info.event = LogEvent.JOB_INFO
    rec_info.jobid = job_id
    rec_info.rule_name = rule
    rec_info.resources = MockResources(threads=2)
    handler.emit(rec_info)

    # Assertions:
    # - Submitted should be 1
    # - Running should IMMEDIATELY become 1 (deferred start processed)
    # - Deferred set should be empty
    assert handler.metric_submitted.labels(rule=rule)._value.get() == 1
    assert handler.metric_running.labels(rule=rule)._value.get() == 1
    assert handler.metric_resource_usage.labels(resource="threads")._value.get() == 2
    assert job_id not in handler.deferred_starts

    # 3. FINISH
    rec_fin = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_fin.event = LogEvent.JOB_FINISHED
    rec_fin.job_id = job_id
    handler.emit(rec_fin)

    # Clean cleanup
    assert handler.metric_submitted.labels(rule=rule)._value.get() == 0
    assert handler.metric_running.labels(rule=rule)._value.get() == 0


def test_standard_lifecycle(handler):
    """Standard Order: INFO -> START -> FINISH"""
    job_id = 1
    rule = "test"

    # Info
    rec_info = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_info.event = LogEvent.JOB_INFO
    rec_info.jobid = job_id
    rec_info.rule_name = rule
    rec_info.threads = 1
    rec_info.resources = MockResources()
    handler.emit(rec_info)

    assert handler.metric_submitted.labels(rule=rule)._value.get() == 1
    assert handler.metric_running.labels(rule=rule)._value.get() == 0

    # Start
    rec_start = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_start.event = LogEvent.JOB_STARTED
    rec_start.jobs = [job_id]
    handler.emit(rec_start)

    assert handler.metric_running.labels(rule=rule)._value.get() == 1

    # Finish
    rec_fin = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_fin.event = LogEvent.JOB_FINISHED
    rec_fin.job_id = job_id
    handler.emit(rec_fin)

    assert handler.metric_submitted.labels(rule=rule)._value.get() == 0
    assert handler.metric_running.labels(rule=rule)._value.get() == 0


def test_missing_start_event(handler):
    """Verify resources/running don't go negative if start event is missed."""
    job_id = 99
    rule = "phantom"

    # Info
    rec_info = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_info.event = LogEvent.JOB_INFO
    rec_info.jobid = job_id
    rec_info.rule_name = rule
    rec_info.threads = 1
    rec_info.resources = MockResources()
    handler.emit(rec_info)

    # START MISSED

    # Finish
    rec_fin = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_fin.event = LogEvent.JOB_FINISHED
    rec_fin.job_id = job_id
    handler.emit(rec_fin)

    assert handler.metric_submitted.labels(rule=rule)._value.get() == 0
    assert handler.metric_running.labels(rule=rule)._value.get() == 0  # Stays 0
    assert handler.metric_finished.labels(status="finished", rule=rule)._value.get() == 1

import logging
import pytest
from unittest.mock import patch
from snakemake_interface_logger_plugins.common import LogEvent
from snakemake_logger_plugin_prometheus import LogHandler, LogHandlerSettings


# Mock Snakemake resources object
class MockResources:
    def __init__(self, **kwargs):
        self._names = list(kwargs.keys())
        for k, v in kwargs.items():
            setattr(self, k, v)
        self._cores = 1
        self._nodes = 1

    def __iter__(self):
        for name in self._names:
            yield getattr(self, name)


class MockOutputSettings:
    def __init__(self):
        self.dryrun = False


@pytest.fixture
def handler(clean_prometheus_registry):
    with patch("snakemake_logger_plugin_prometheus.start_http_server"):
        settings = LogHandlerSettings(port=9091)
        common_settings = MockOutputSettings()
        h = LogHandler(common_settings, settings)
        yield h
        h.close()


def test_dynamic_checkpoints(handler):
    """
    Test that the metrics correctly update if Snakemake emits new DAG info
    (e.g., after a checkpoint is evaluated).
    """
    # 1. Initial Plan
    rec_run1 = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_run1.event = LogEvent.RUN_INFO
    rec_run1.per_rule_job_counts = {"ruleA": 5, "ruleB": 5}
    handler.emit(rec_run1)

    assert handler.metric_planned.labels(rule="ruleA")._value.get() == 5
    assert handler.metric_planned.labels(rule="ruleB")._value.get() == 5

    # 2. Checkpoint Triggered! DAG Re-evaluated.
    # New plan: ruleA is done (or same), ruleB increased to 50.
    rec_run2 = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_run2.event = LogEvent.RUN_INFO
    rec_run2.per_rule_job_counts = {"ruleA": 5, "ruleB": 50}
    handler.emit(rec_run2)

    # 3. Verify metrics updated
    assert handler.metric_planned.labels(rule="ruleA")._value.get() == 5
    assert handler.metric_planned.labels(rule="ruleB")._value.get() == 50

    # 4. Progress Total updates (aggregate)
    rec_prog = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_prog.event = LogEvent.PROGRESS
    rec_prog.done = 5
    rec_prog.total = 55  # Updated total
    handler.emit(rec_prog)

    assert handler.metric_progress.labels(type="total")._value.get() == 55


def test_workflow_metadata_events(handler):
    """
    Test events that provide global workflow info:
    RESOURCES_INFO, RUN_INFO, PROGRESS
    """
    # 1. RESOURCES_INFO (Limits)
    rec_res = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_res.event = LogEvent.RESOURCES_INFO
    rec_res.cores = 8
    rec_res.provided_resources = {"mem_mb": 16000, "gpu": 2}
    handler.emit(rec_res)

    assert handler.metric_resource_limits.labels(resource="threads")._value.get() == 8
    assert handler.metric_resource_limits.labels(resource="mem_mb")._value.get() == 16000

    # 2. PROGRESS (Done/Total)
    rec_prog = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    rec_prog.event = LogEvent.PROGRESS
    rec_prog.done = 3
    rec_prog.total = 15
    handler.emit(rec_prog)

    assert handler.metric_progress.labels(type="done")._value.get() == 3
    assert handler.metric_progress.labels(type="total")._value.get() == 15


def test_metric_updates_lifecycle(handler):
    # 1. Job Info
    record_info = logging.LogRecord("snakemake", logging.INFO, "x", 1, "Job info", (), None)
    record_info.event = LogEvent.JOB_INFO
    record_info.jobid = 1
    record_info.rule_name = "test_rule"
    record_info.threads = 4
    record_info.resources = MockResources(mem_mb=1000)
    handler.emit(record_info)

    assert handler.metric_running.labels(rule="test_rule")._value.get() == 0
    assert handler.metric_resource_usage.labels(resource="threads")._value.get() == 0

    # 2. Job Started
    record_start = logging.LogRecord("snakemake", logging.INFO, "x", 1, "Job started", (), None)
    record_start.event = LogEvent.JOB_STARTED
    record_start.jobs = [1]
    handler.emit(record_start)

    assert handler.metric_running.labels(rule="test_rule")._value.get() == 1
    assert handler.metric_resource_usage.labels(resource="threads")._value.get() == 4
    assert handler.metric_resource_usage.labels(resource="mem_mb")._value.get() == 1000

    # 3. Job Finished
    record_finish = logging.LogRecord("snakemake", logging.INFO, "x", 1, "Job finished", (), None)
    record_finish.event = LogEvent.JOB_FINISHED
    record_finish.job_id = 1
    handler.emit(record_finish)

    assert handler.metric_running.labels(rule="test_rule")._value.get() == 0
    assert handler.metric_finished.labels(status="finished", rule="test_rule")._value.get() == 1
    assert handler.metric_resource_usage.labels(resource="threads")._value.get() == 0


def test_job_failure(handler):
    # Info
    record_info = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    record_info.event = LogEvent.JOB_INFO
    record_info.jobid = 2
    record_info.rule_name = "failing_rule"
    record_info.resources = MockResources(threads=2)
    handler.emit(record_info)

    # Start
    record_start = logging.LogRecord("snakemake", logging.INFO, "x", 1, "msg", (), None)
    record_start.event = LogEvent.JOB_STARTED
    record_start.jobs = [2]
    handler.emit(record_start)

    assert handler.metric_running.labels(rule="failing_rule")._value.get() == 1

    # Fail
    record_error = logging.LogRecord("snakemake", logging.ERROR, "x", 1, "msg", (), None)
    record_error.event = LogEvent.JOB_ERROR
    record_error.jobid = 2
    handler.emit(record_error)

    assert handler.metric_running.labels(rule="failing_rule")._value.get() == 0
    assert handler.metric_finished.labels(status="failed", rule="failing_rule")._value.get() == 1
    assert handler.metric_resource_usage.labels(resource="threads")._value.get() == 0

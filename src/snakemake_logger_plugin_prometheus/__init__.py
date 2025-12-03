from dataclasses import dataclass, field
import logging
import threading
import sys
from typing import Any

from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase
from snakemake_interface_logger_plugins.common import LogEvent
from prometheus_client import (
    start_http_server,
    Gauge,
    Counter,
    push_to_gateway,
    REGISTRY,
)


@dataclass
class LogHandlerSettings(LogHandlerSettingsBase):
    port: int = field(
        default=8000,
        metadata={
            "help": "Port to expose Prometheus metrics on",
            "env_var": False,
            "required": False,
            "type": int,
        },
    )
    push_gateway: str | None = field(
        default=None,
        metadata={
            "help": "URL of the Prometheus Pushgateway (e.g. http://localhost:9091)",
            "env_var": False,
            "required": False,
            "type": str,
        },
    )
    push_job_name: str = field(
        default="snakemake",
        metadata={
            "help": "Job name for the Pushgateway grouping",
            "env_var": False,
            "required": False,
            "type": str,
        },
    )


@dataclass
class JobMetadata:
    """
    Structured container for job information extracted from JOB_INFO logs.
    """

    job_id: int
    rule_name: str
    resources: dict[str, int | float]
    threads: int


class LogHandler(LogHandlerBase):
    def __post_init__(self) -> None:
        self.job_registry: dict[int, JobMetadata] = {}
        self.running_jobs: set[int] = set()
        self.deferred_starts: set[int] = set()

        self.metric_submitted = Gauge(
            "snakemake_jobs_submitted",
            "Number of jobs known to the scheduler (Queued + Running).",
            ["rule"],
        )

        self.metric_running = Gauge(
            "snakemake_jobs_running",
            "Number of jobs actively executing.",
            ["rule"],
        )

        self.metric_finished = Counter(
            "snakemake_jobs_finished_total",
            "Total number of jobs finished/failed.",
            ["status", "rule"],
        )

        self.metric_resource_usage = Gauge(
            "snakemake_resource_usage_total",
            "Total resources currently consumed by RUNNING jobs.",
            ["resource"],
        )

        self.metric_resource_limits = Gauge(
            "snakemake_resource_limits",
            "Maximum resources available to the workflow execution.",
            ["resource"],
        )

        self.metric_progress = Gauge(
            "snakemake_workflow_progress",
            "High-level workflow progress (jobs done vs total).",
            ["type"],  # 'done', 'total'
        )

        self.metric_planned = Gauge(
            "snakemake_jobs_planned_total", "Jobs planned per rule. Updates on DAG re-evaluation.", ["rule"]
        )

        self.metric_errors = Counter("snakemake_errors_total", "Total number of workflow errors.")

        self._setup_server()
        self._setup_push_gateway()

    def _setup_server(self) -> None:
        port = getattr(self.settings, "port", 8000)
        try:
            start_http_server(port)
            sys.stderr.write(f"[PrometheusPlugin] Metrics server started on port {port}\n")
        except OSError as e:
            sys.stderr.write(f"[PrometheusPlugin] ⚠️ Could not start server on port {port}: {e}\n")

    def _setup_push_gateway(self) -> None:
        self._stop_push_event = threading.Event()
        push_gateway = getattr(self.settings, "push_gateway", None)

        if push_gateway:
            push_job_name = getattr(self.settings, "push_job_name", "snakemake")
            sys.stderr.write(f"[PrometheusPlugin] Pushing metrics to {push_gateway} (job: {push_job_name})\n")
            self._push_thread = threading.Thread(target=self._push_loop, daemon=True)
            self._push_thread.start()

    def _push_loop(self) -> None:
        """Periodically pushes metrics to the Pushgateway."""
        push_gateway = getattr(self.settings, "push_gateway", None)
        assert push_gateway is not None
        push_job_name = getattr(self.settings, "push_job_name", "snakemake")

        while not self._stop_push_event.is_set():
            try:
                push_to_gateway(push_gateway, job=push_job_name, registry=REGISTRY)
            except Exception as e:
                sys.stderr.write(f"[PrometheusPlugin] Push failed: {e}\n")

            if self._stop_push_event.wait(15):
                break

    def close(self) -> None:
        if hasattr(self, "_stop_push_event"):
            self._stop_push_event.set()
            if hasattr(self, "_push_thread") and self._push_thread.is_alive():
                self._push_thread.join(timeout=1.0)

        self._cleanup_registry()
        super().close()

    def _cleanup_registry(self) -> None:
        """Unregister metrics to prevent state bleeding in test suites."""
        metrics = [
            self.metric_submitted,
            self.metric_running,
            self.metric_finished,
            self.metric_resource_usage,
            self.metric_resource_limits,
            self.metric_progress,
            self.metric_planned,
            self.metric_errors,
        ]
        for m in metrics:
            try:
                REGISTRY.unregister(m)
            except (KeyError, AttributeError, TypeError):
                pass

    @property
    def writes_to_stream(self) -> bool:
        return False

    @property
    def writes_to_file(self) -> bool:
        return False

    @property
    def has_filter(self) -> bool:
        return True

    @property
    def has_formatter(self) -> bool:
        return True

    @property
    def needs_rulegraph(self) -> bool:
        return False

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if not hasattr(record, "event"):
                return

            event = record.event

            if event == LogEvent.JOB_INFO:
                self._handle_job_info(record)
            elif event == LogEvent.JOB_STARTED:
                self._handle_job_started(record)
            elif event == LogEvent.JOB_FINISHED:
                self._handle_job_finished(record)
            elif event == LogEvent.JOB_ERROR:
                self._handle_job_error(record)
            elif event == LogEvent.ERROR:
                self.metric_errors.inc()
            elif event == LogEvent.RUN_INFO:
                self._handle_run_info(record)
            elif event == LogEvent.PROGRESS:
                self._handle_progress(record)
            elif event == LogEvent.RESOURCES_INFO:
                self._handle_resources_info(record)

        except Exception:
            self.handleError(record)

    def _parse_resources(self, record: logging.LogRecord) -> dict[str, int | float]:
        """
        Robustly extracts resources from a LogRecord.
        Handles: dicts, argparse.Namespace, and Snakemake's internal Resource objects.
        """
        raw_res = getattr(record, "resources", None)
        resources: dict[str, Any] = {}

        if raw_res:
            if hasattr(raw_res, "_names"):
                for name in raw_res._names:
                    resources[name] = getattr(raw_res, name)
            elif isinstance(raw_res, dict):
                resources = raw_res.copy()
            elif hasattr(raw_res, "__dict__"):
                resources = vars(raw_res).copy()
            elif hasattr(raw_res, "keys") and callable(raw_res.keys):
                for k in raw_res.keys():
                    resources[k] = raw_res[k]

        filtered: dict[str, int | float] = {
            k: v for k, v in resources.items() if k not in {"_cores", "_nodes"} and isinstance(v, (int, float))
        }

        if "threads" not in filtered:
            threads = getattr(record, "threads", 1)
            if isinstance(threads, (int, float)):
                filtered["threads"] = threads

        return filtered

    def _handle_run_info(self, record: logging.LogRecord) -> None:
        """Handles run info (DAG stats). Can be called multiple times if DAG is re-evaluated."""
        counts = getattr(record, "per_rule_job_counts", {})
        if counts:
            for rule, count in counts.items():
                self.metric_planned.labels(rule=rule).set(count)

    def _handle_progress(self, record: logging.LogRecord) -> None:
        """Handles progress updates. 'total' can change dynamically via checkpoints."""
        done = getattr(record, "done", 0)
        total = getattr(record, "total", 0)
        self.metric_progress.labels(type="done").set(done)
        self.metric_progress.labels(type="total").set(total)

    def _handle_resources_info(self, record: logging.LogRecord) -> None:
        """Handles the global resources info to set usage limits."""
        cores = getattr(record, "cores", None)
        if cores is not None:
            self.metric_resource_limits.labels(resource="threads").set(cores)
            self.metric_resource_limits.labels(resource="cores").set(cores)

        provided = getattr(record, "provided_resources", {})
        if provided:
            for res, value in provided.items():
                if isinstance(value, (int, float)):
                    self.metric_resource_limits.labels(resource=res).set(value)

    def _handle_job_info(self, record: logging.LogRecord) -> None:
        job_id = getattr(record, "jobid", None)
        if job_id is None:
            return

        rule_name = getattr(record, "rule_name", "unknown")
        resources = self._parse_resources(record)
        threads = int(resources.get("threads", 1))

        metadata = JobMetadata(job_id=job_id, rule_name=rule_name, resources=resources, threads=threads)

        self.job_registry[job_id] = metadata

        self.metric_submitted.labels(rule=rule_name).inc()

        if job_id in self.deferred_starts:
            self.deferred_starts.remove(job_id)
            self._record_job_start(metadata)

    def _handle_job_started(self, record: logging.LogRecord) -> None:
        job_ids = getattr(record, "job_ids", [])

        if not job_ids:
            job_ids = getattr(record, "jobs", [])

        if isinstance(job_ids, int):
            job_ids = [job_ids]
        elif not job_ids and hasattr(record, "jobid"):
            jid = getattr(record, "jobid")
            if jid is not None:
                job_ids = [jid]

        for job_id in job_ids:
            if not isinstance(job_id, int):
                continue

            if job_id in self.job_registry:
                self._record_job_start(self.job_registry[job_id])
            else:
                self.deferred_starts.add(job_id)

    def _record_job_start(self, metadata: JobMetadata) -> None:
        """Internal helper to increment running metrics."""
        if metadata.job_id in self.running_jobs:
            return

        self.metric_running.labels(rule=metadata.rule_name).inc()

        for res_name, res_value in metadata.resources.items():
            self.metric_resource_usage.labels(resource=res_name).inc(res_value)

        self.running_jobs.add(metadata.job_id)

    def _handle_job_finished(self, record: logging.LogRecord) -> None:
        job_id = getattr(record, "job_id", getattr(record, "jobid", None))
        if job_id is not None:
            self._finalize_job(job_id, "finished")

    def _handle_job_error(self, record: logging.LogRecord) -> None:
        job_id = getattr(record, "jobid", None)
        if job_id is not None:
            self._finalize_job(job_id, "failed")

    def _finalize_job(self, job_id: int, final_status: str) -> None:
        job_info = self.job_registry.pop(job_id, None)

        if job_id in self.deferred_starts:
            self.deferred_starts.remove(job_id)

        if job_info:
            rule = job_info.rule_name

            self.metric_finished.labels(status=final_status, rule=rule).inc()

            self.metric_submitted.labels(rule=rule).dec()

            if job_id in self.running_jobs:
                self.metric_running.labels(rule=rule).dec()

                for res_name, res_value in job_info.resources.items():
                    self.metric_resource_usage.labels(resource=res_name).dec(res_value)

                self.running_jobs.remove(job_id)

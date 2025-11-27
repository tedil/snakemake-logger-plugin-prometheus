from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import logging
import threading
import sys
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
        },
    )
    push_gateway: Optional[str] = field(
        default=None,
        metadata={
            "help": "URL of the Prometheus Pushgateway (e.g. http://localhost:9091)",
            "env_var": False,
            "required": False,
        },
    )
    push_job_name: str = field(
        default="snakemake",
        metadata={
            "help": "Job name for the Pushgateway grouping",
            "env_var": False,
            "required": False,
        },
    )


class LogHandler(LogHandlerBase):
    def __post_init__(self) -> None:
        self.job_registry: Dict[int, Dict[str, Any]] = {}
        self.running_jobs: Dict[int, Dict[str, Any]] = {}

        self.metric_running = Gauge(
            "snakemake_jobs_running",
            "Number of jobs currently running",
            ["rule"],
        )

        self.metric_finished = Counter(
            "snakemake_jobs_finished_total",
            "Total number of jobs finished/failed",
            ["status", "rule"],
        )

        self.metric_resource_usage = Gauge(
            "snakemake_resource_usage_total",
            "Total resources currently reserved by running jobs",
            ["resource"],
        )

        self.metric_resource_limits = Gauge(
            "snakemake_resource_limits",
            "Maximum resources available to the workflow execution",
            ["resource"],
        )

        self.metric_progress = Gauge(
            "snakemake_workflow_progress",
            "High-level workflow progress (jobs done vs total). Updates dynamically on checkpoints.",
            ["type"],  # 'done', 'total'
        )

        self.metric_planned = Gauge(
            "snakemake_jobs_planned_total",
            "Jobs planned per rule. May update during execution if checkpoints trigger DAG re-evaluation.",
            ["rule"],
        )

        self.metric_errors = Counter("snakemake_errors_total", "Total number of workflow errors")

        self.server_started = False
        port = getattr(self.settings, "port", 8000)
        try:
            # start_http_server spawns a daemon thread
            start_http_server(port)
            self.server_started = True
            sys.stderr.write(f"[PrometheusPlugin] Metrics server started on port {port}\n")
        except OSError as e:
            sys.stderr.write(f"[PrometheusPlugin] ⚠️ Could not start server on port {port}: {e}\n")

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
                push_to_gateway(
                    push_gateway,
                    job=push_job_name,
                    registry=REGISTRY,
                )
            except Exception as e:
                sys.stderr.write(f"[PrometheusPlugin] Push failed: {e}\n")

            if self._stop_push_event.wait(15):
                break

    def close(self) -> None:
        """Cleanup logic called when Snakemake shuts down."""
        # Stop push thread if running
        if hasattr(self, "_stop_push_event"):
            self._stop_push_event.set()
            if hasattr(self, "_push_thread") and self._push_thread.is_alive():
                try:
                    push_gateway = getattr(self.settings, "push_gateway", None)
                    push_job_name = getattr(self.settings, "push_job_name", "snakemake")
                    if push_gateway:
                        push_to_gateway(push_gateway, job=push_job_name, registry=REGISTRY)
                except Exception:
                    pass
                self._push_thread.join(timeout=1.0)

        try:
            REGISTRY.unregister(self.metric_running)
            REGISTRY.unregister(self.metric_finished)
            REGISTRY.unregister(self.metric_resource_usage)
            REGISTRY.unregister(self.metric_resource_limits)
            REGISTRY.unregister(self.metric_progress)
            REGISTRY.unregister(self.metric_planned)
            REGISTRY.unregister(self.metric_errors)
        except (KeyError, AttributeError, TypeError):
            pass

        super().close()

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

    def _handle_run_info(self, record: logging.LogRecord) -> None:
        """Handles run info (DAG stats). Can be called multiple times if DAG is re-evaluated."""
        per_rule_counts = getattr(record, "per_rule_job_counts", {})
        if per_rule_counts:
            for rule, count in per_rule_counts.items():
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

        provided_resources = getattr(record, "provided_resources", {})
        if provided_resources:
            for res, value in provided_resources.items():
                if isinstance(value, (int, float)):
                    self.metric_resource_limits.labels(resource=res).set(value)

    def _handle_job_info(self, record: logging.LogRecord) -> None:
        job_id = getattr(record, "jobid", None)
        if job_id is None:
            return

        raw_resources = getattr(record, "resources", None)
        resources = {}

        if raw_resources:
            if isinstance(raw_resources, dict):
                resources = raw_resources.copy()
            elif hasattr(raw_resources, "__dict__"):
                resources = vars(raw_resources).copy()
            elif hasattr(raw_resources, "_names"):
                resources = {name: getattr(raw_resources, name) for name in raw_resources._names}

        resources = {k: v for k, v in resources.items() if k not in {"_cores", "_nodes"}}
        if "threads" not in resources:
            resources["threads"] = getattr(record, "threads", 1)

        self.job_registry[job_id] = {
            "rule": getattr(record, "rule_name", "unknown"),
            "resources": resources,
        }

    def _handle_job_started(self, record: logging.LogRecord) -> None:
        job_ids = getattr(record, "jobs", [])

        if isinstance(job_ids, int):
            job_ids = [job_ids]

        if not job_ids:
            if hasattr(record, "job_ids"):
                job_ids = record.job_ids
            elif hasattr(record, "jobid"):
                job_ids = [record.jobid]

        for job_id in job_ids:
            job_info = self.job_registry.get(job_id)
            if not job_info:
                continue

            rule_name = job_info["rule"]
            self.metric_running.labels(rule=rule_name).inc()

            for res_name, res_value in job_info["resources"].items():
                if isinstance(res_value, (int, float)):
                    self.metric_resource_usage.labels(resource=res_name).inc(res_value)

            self.running_jobs[job_id] = job_info

    def _handle_job_finished(self, record: logging.LogRecord) -> None:
        job_id = getattr(record, "job_id", getattr(record, "jobid", None))
        if job_id is None:
            return
        self._finalize_job(job_id, "finished")

    def _handle_job_error(self, record: logging.LogRecord) -> None:
        job_id = getattr(record, "jobid", None)
        if job_id is None:
            return
        self._finalize_job(job_id, "failed")

    def _finalize_job(self, job_id: int, final_status: str) -> None:
        job_info = self.running_jobs.pop(job_id, None)

        if not job_info:
            job_info = self.job_registry.get(job_id)

        if job_info:
            rule_name = job_info["rule"]

            self.metric_finished.labels(status=final_status, rule=rule_name).inc()

            if job_info:
                self.metric_running.labels(rule=rule_name).dec()

                for res_name, res_value in job_info["resources"].items():
                    if isinstance(res_value, (int, float)):
                        self.metric_resource_usage.labels(resource=res_name).dec(res_value)

            self.job_registry.pop(job_id, None)

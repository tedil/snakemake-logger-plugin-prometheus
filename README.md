# Snakemake Prometheus Logger Plugin

A Snakemake logger plugin that exposes workflow metrics via a Prometheus-compatible HTTP endpoint.

## Installation

You can install this plugin via pip:

```sh
pip install snakemake-logger-plugin-prometheus
```

Or via Pixi/Conda if available in your channels.

## Usage

To use this logger, run Snakemake with the `--logger prometheus` flag.

```sh
snakemake --logger prometheus ...
```

### Pull Mode (Default)

By default, the metrics server runs on port `8000`. Prometheus scrapes this address.
You can change the port using the `--logger-prometheus-port` flag:

```sh
snakemake --logger prometheus --logger-prometheus-port 9090 ...
```

### Push Mode (Pushgateway)

If you cannot expose a port or prefer pushing metrics, specify a Pushgateway URL.
The plugin will push metrics every 15 seconds.

```sh
snakemake --logger prometheus --logger-prometheus-push-gateway http://localhost:9091
```

You can optionally set the job name used in the Pushgateway (default: `"snakemake"`):

```sh
snakemake --logger prometheus --logger-prometheus-push-gateway http://localhost:9091 --logger-prometheus-push-job-name my-workflow
```

## Available Metrics

### Job Lifecycle

| Metric Name                     | Type    | Labels                             | Description                                                                                               |
|:--------------------------------|:--------|:-----------------------------------|:----------------------------------------------------------------------------------------------------------|
| `snakemake_jobs_submitted`      | Gauge   | `rule`                             | Jobs known to the scheduler (Queued + Running). Increments when a job is ready, decrements when finished. |
| `snakemake_jobs_running`        | Gauge   | `rule`                             | Jobs actively executing. Increments when execution starts, decrements when finished.                      |
| `snakemake_jobs_finished_total` | Counter | `status` (finished/failed), `rule` | Cumulative count of completed jobs. Useful for throughput rates.                                          |
| `snakemake_jobs_planned_total`  | Gauge   | `rule`                             | Total jobs planned for the workflow. Updates dynamically if checkpoints trigger DAG re-evaluation.        |

### Resources

| Metric Name                      | Type  | Labels     | Description                                                                         |
|:---------------------------------|:------|:-----------|:------------------------------------------------------------------------------------|
| `snakemake_resource_usage_total` | Gauge | `resource` | Total resources currently consumed by **Running** jobs (e.g., `threads`, `mem_mb`). |
| `snakemake_resource_limits`      | Gauge | `resource` | Maximum resources available to the workflow (CLI limits or cluster capacity).       |

### Workflow Status

| Metric Name                   | Type    | Labels              | Description                            |
|:------------------------------|:--------|:--------------------|:---------------------------------------|
| `snakemake_workflow_progress` | Gauge   | `type` (done/total) | High-level progress counters.          |
| `snakemake_errors_total`      | Counter | None                | Total number of workflow-level errors. |

## Grafana Query Examples

Here are common queries to visualize this data:

| Description                  | Query                                                                                                                |
|:-----------------------------|:---------------------------------------------------------------------------------------------------------------------|
| **Queued Jobs**              | `sum(snakemake_jobs_submitted) - sum(snakemake_jobs_running)`                                                        |
| **Running Jobs**             | `sum(snakemake_jobs_running)`                                                                                        |
| **Throughput (Jobs/Minute)** | `sum(rate(snakemake_jobs_finished_total{status="finished"}[5m])) * 60`                                               |
| **Resource Utilization %**   | `sum(snakemake_resource_usage_total{resource="threads"}) / sum(snakemake_resource_limits{resource="threads"}) * 100` |
| **Failure Rate**             | `sum(rate(snakemake_jobs_finished_total{status="failed"}[5m]))`                                                      |

## Development

This project uses [pixi](https://pixi.sh) for dependency management and [uv](https://docs.astral.sh/uv/) for
building/publishing.

- Install dependencies: `pixi install`
- Run tests: `pixi run test`
- Run code formatting and linting: `pixi run qc`
- Build package: `pixi run build`
- Publish package: `pixi run publish`
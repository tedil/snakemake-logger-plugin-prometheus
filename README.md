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

| Metric Name                      | Type    | Labels                             | Description                                         |
|:---------------------------------|:--------|:-----------------------------------|:----------------------------------------------------|
| **Job Execution**                |         |                                    |                                                     |
| `snakemake_jobs_running`         | Gauge   | `rule`                             | Current number of jobs actively running.            |
| `snakemake_jobs_finished_total`  | Counter | `status` (finished/failed), `rule` | Cumulative count of completed jobs.                 |
| `snakemake_jobs_planned_total`   | Gauge   | `rule`                             | Total jobs planned for this execution.              |
| **Resources**                    |         |                                    |                                                     |
| `snakemake_resource_usage_total` | Gauge   | `resource`                         | Total resources currently reserved by running jobs. |
| `snakemake_resource_limits`      | Gauge   | `resource`                         | Maximum resources available (e.g. CLI limits).      |
| **Workflow Status**              |         |                                    |                                                     |
| `snakemake_workflow_progress`    | Gauge   | `type` (done/total)                | High-level progress counters.                       |
| `snakemake_errors_total`         | Counter | None                               | Total number of workflow-level errors.              |

### Grafana Query Examples

| Description               | Query                                                                                                                |
|:--------------------------|:---------------------------------------------------------------------------------------------------------------------|
| **Cluster Utilization %** | `sum(snakemake_resource_usage_total{resource="threads"}) / sum(snakemake_resource_limits{resource="threads"}) * 100` |
| **Workflow Progress %**   | `snakemake_workflow_progress{type="done"} / snakemake_workflow_progress{type="total"} * 100`                         |
| **Throughput (Jobs/Min)** | `sum(rate(snakemake_jobs_finished_total{status="finished"}[5m])) * 60`                                               |

## Development

This project uses pixi for development.

- Install dependencies: `pixi install`
- Run tests: `pixi run test`
- Run code formatting and linting: `pixi run qc`
- Build package: `pixi run build`
- Publish package: `pixi run publish`
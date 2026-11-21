# Dagster threaded executor

Run independent steps on threads in one run worker. Supports stock Dagster **1.10–1.13** and Python **3.11–3.14**, subject to the Dagster release's Python requirements.

## Install

In your uv project, run:

```sh
uv add 'dagster-threaded-executor @ https://github.com/gajop/dagster-threaded-executor/releases/download/v0.2.0/dagster_threaded_executor-0.2.0-py3-none-any.whl'
```

Or add this to your existing `pyproject.toml` and run `uv sync`.

```toml
[project]
dependencies = ["dagster-threaded-executor"]

[tool.uv.sources]
dagster-threaded-executor = { url = "https://github.com/gajop/dagster-threaded-executor/releases/download/v0.2.0/dagster_threaded_executor-0.2.0-py3-none-any.whl" }
```

With pip, pass the same wheel URL to `python -m pip install`.

## Select the executor

Install wherever Dagster imports your definitions or executes runs. On stock Dagster, select the custom executor once in `Definitions`. Jobs with an explicit executor keep it.

```python
from dagster_threaded_executor import threaded_executor

defs = dg.Definitions(jobs=[my_job], executor=threaded_executor)
```

**There is no executor concurrency limit by default.** Dagster concurrency pools still apply. For an optional per-run limit, use [run YAML](examples/run.yaml).

```yaml
execution:
  config:
    max_concurrent: 2
```

Or set a fixed limit on the executor in `Definitions` and omit run YAML.

```python
executor = threaded_executor.configured({"max_concurrent": 2}, name="threaded_default")
defs = dg.Definitions(jobs=[my_job], executor=executor)
```

Shared resources and I/O managers must be thread-safe. Cancellation waits for running steps to exit.

### YAML-only setup with the fork

Stock Dagster run YAML configures the executor selected in `Definitions`. The [Dagster fork](https://github.com/gajop/dagster/tree/threaded-executor) can select it in `dagster.yaml` for jobs using Dagster's default executor.

```yaml
default_executor:
  threaded: {}
```

Add `max_concurrent` under `threaded` if you want a limit.

## Run the example

From a checkout, run [two independent synchronous ops](examples/definitions.py).

```sh
uv run dagster job execute -f examples/definitions.py -a defs -j two_waits
```

## BigQuery benchmark

![Community async, threaded, and stock multiprocess compared on elapsed time and peak memory](benchmark/multiprocess-comparison.png)

Each case ran 50 identical sync ops on local Linux with Python 3.13 and Dagster 1.13.31. Each op submits [one five-second server-side wait](benchmark/wait.sql) and polls job status. The queries scanned and billed zero bytes. Wall time includes startup. Peak memory sums process-tree PSS and proportional swap. The three async cases ran concurrently. [Measured results](benchmark/results.csv)

From a checkout, configure Google Application Default Credentials with a BigQuery project, then run:

```sh
python -m pip install '.[benchmark]'
python benchmark/profile.py
python benchmark/compare.py
```

The full run takes about 15 minutes. Results and the regenerated chart go in `benchmark/runs/`.

import asyncio
import os
import threading

import dagster as dg
from dagster._core.execution.plan.active import ActiveExecution

from dagster_threaded_executor import threaded_executor

TWO_THREADS = {"execution": {"config": {"max_concurrent": 2}}}


@dg.resource
def barrier():
    return threading.Barrier(2, timeout=5)


@dg.op(required_resource_keys={"barrier"})
def worker_identity(context: dg.OpExecutionContext):
    context.resources.barrier.wait()
    return os.getpid(), threading.get_ident()


@dg.op
def verify_workers(first, second):
    assert first[0] == second[0] == os.getpid()
    assert first[1] != second[1]


@dg.job(resource_defs={"barrier": barrier, "io_manager": dg.mem_io_manager})
def parallel_job():
    verify_workers(worker_identity.alias("first")(), worker_identity.alias("second")())


defs = dg.Definitions(jobs=[parallel_job], executor=threaded_executor)


def load_parallel_job():
    return defs.get_repository_def().get_job("parallel_job")


def test_definitions_executor_runs_independent_ops_on_two_threads():
    assert load_parallel_job().executor_def is threaded_executor
    with dg.instance_for_test() as instance:
        result = dg.execute_job(
            dg.reconstructable(load_parallel_job),
            instance=instance,
            run_config=TWO_THREADS,
        )
        assert result.success


@dg.resource
def wide_barrier():
    return threading.Barrier(40, timeout=15)


@dg.job(
    executor_def=threaded_executor,
    resource_defs={"barrier": wide_barrier, "io_manager": dg.mem_io_manager},
)
def wide_parallel_job():
    for i in range(40):
        worker_identity.alias(f"step_{i}")()


def test_default_has_no_executor_concurrency_limit():
    with dg.instance_for_test() as instance:
        result = dg.execute_job(
            dg.reconstructable(wide_parallel_job), instance=instance
        )
        assert result.success
        assert sum(event.is_step_success for event in result.all_events) == 40


@dg.op(out=dg.DynamicOut(int))
def fan_out():
    for value in (1, 2):
        yield dg.DynamicOutput(value, mapping_key=str(value))


@dg.op
async def double_with_retry(context: dg.OpExecutionContext, value: int) -> int:
    await asyncio.sleep(0.01)
    if context.retry_number == 0:
        raise dg.RetryRequested(max_retries=1)
    return 2 * value


@dg.op
def verify_outputs(values: list[int]):
    assert sorted(values) == [2, 4]


@dg.job(executor_def=threaded_executor)
def mapped_job():
    verify_outputs(fan_out().map(double_with_retry).collect())


def test_async_steps_retries_and_dynamic_mapping():
    with dg.instance_for_test() as instance:
        result = dg.execute_job(
            dg.reconstructable(mapped_job), instance=instance, run_config=TWO_THREADS
        )
        assert result.success
        retries = {
            event.step_key for event in result.all_events if event.is_step_up_for_retry
        }
        assert retries == {"double_with_retry[1]", "double_with_retry[2]"}


started = threading.Event()
release = threading.Event()


@dg.op(pool="test_pool")
def wait_for_interrupt():
    started.set()
    assert release.wait(timeout=5)


@dg.op
def should_not_start(upstream):
    raise AssertionError("A downstream step ran after interruption")


@dg.job(executor_def=threaded_executor)
def interrupted_job():
    should_not_start(wait_for_interrupt())


def test_interrupt_drains_threads_and_releases_pool_slot(monkeypatch):
    started.clear()
    release.clear()

    def check_for_interrupts(_active):
        if not started.is_set():
            return False
        release.set()
        return True

    monkeypatch.setattr(ActiveExecution, "check_for_interrupts", check_for_interrupts)
    with dg.instance_for_test() as instance:
        instance.event_log_storage.set_concurrency_slots("test_pool", 1)
        result = dg.execute_job(
            dg.reconstructable(interrupted_job),
            instance=instance,
            run_config=TWO_THREADS,
        )
        assert not result.success
        step_events = instance.all_logs(
            result.run_id, of_type=dg.DagsterEventType.STEP_START
        )
        assert [event.step_key for event in step_events] == ["wait_for_interrupt"]
        assert (
            instance.event_log_storage.get_concurrency_info(
                "test_pool"
            ).active_slot_count
            == 0
        )

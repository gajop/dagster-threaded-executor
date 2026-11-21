# Adapted from the Dagster execution engine under the Apache License 2.0.
from __future__ import annotations

import asyncio
import sys
from collections.abc import Iterator
from concurrent.futures import Future
from contextlib import ExitStack
from copy import copy
from functools import partial
from queue import Empty, SimpleQueue
from typing import TYPE_CHECKING

import dagster as dg
import dagster._check as check
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.events import DagsterEvent
from dagster._core.execution.api import ExecuteRunWithPlanIterable
from dagster._core.execution.compute_logs import create_compute_log_file_key
from dagster._core.execution.context.system import (
    PlanExecutionContext,
    PlanOrchestrationContext,
    StepExecutionContext,
)
from dagster._core.execution.context_creation_job import PlanExecutionContextManager
from dagster._core.execution.plan.execute_plan import (
    _handle_compute_log_setup_error,
    _handle_compute_log_teardown_error,
    _trigger_hook,
    dagster_event_sequence_for_step,
)
from dagster._core.execution.plan.instance_concurrency_context import (
    InstanceConcurrencyContext,
)
from dagster._core.execution.plan.plan import ExecutionPlan
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.executor.in_process import InProcessExecutor
from dagster._core.utils import InheritContextThreadPoolExecutor

if TYPE_CHECKING:
    from dagster._core.execution.plan.active import ActiveExecution
    from dagster._core.execution.plan.state import KnownExecutionState
    from dagster._core.execution.plan.step import ExecutionStep
    from dagster._core.executor.init import InitExecutorContext


def _execution_iterator(
    job_context: PlanExecutionContext,
    execution_plan: ExecutionPlan,
    instance_concurrency_context: InstanceConcurrencyContext,
    *,
    max_concurrent: int,
) -> Iterator[DagsterEvent]:
    check.inst_param(job_context, "pipeline_context", PlanExecutionContext)
    check.inst_param(execution_plan, "execution_plan", ExecutionPlan)
    compute_log_manager = job_context.instance.compute_log_manager
    step_keys = [
        step.key for step in execution_plan.get_steps_to_execute_in_topo_order()
    ]
    with (
        execution_plan.start(
            retry_mode=job_context.retry_mode,
            max_concurrent=max_concurrent,
            instance_concurrency_context=instance_concurrency_context,
            step_dependency_config=job_context.step_dependency_config,
        ) as active_execution,
        ExitStack() as capture_stack,
    ):
        # Capture stdout and stderr once for the whole run process.
        file_key = create_compute_log_file_key()
        log_key = compute_log_manager.build_log_key_for_run(
            job_context.run_id, file_key
        )
        try:
            log_context = capture_stack.enter_context(
                compute_log_manager.capture_logs(log_key)
            )
            yield DagsterEvent.capture_logs(
                job_context, step_keys, log_key, log_context
            )
        except Exception:  # noqa: BLE001 - Dagster records compute log errors as events.
            yield from _handle_compute_log_setup_error(job_context, sys.exc_info())

        yield from _execute_steps_on_threads(
            job_context, active_execution, max_concurrent
        )

        try:
            capture_stack.close()
        except Exception:  # noqa: BLE001 - Dagster records compute log errors as events.
            yield from _handle_compute_log_teardown_error(job_context, sys.exc_info())


def _validate_step_resources(step_context: StepExecutionContext) -> None:
    missing_resources = [
        resource_key
        for resource_key in step_context.required_resource_keys
        if not hasattr(step_context.resources, resource_key)
    ]
    check.invariant(
        len(missing_resources) == 0,
        (
            f"Expected step context for solid {step_context.op.name} to have all required"
            f" resources, but missing {missing_resources}."
        ),
    )


def _execute_steps_on_threads(
    job_context: PlanExecutionContext,
    active_execution: ActiveExecution,
    max_concurrent: int,
) -> Iterator[DagsterEvent]:
    events: SimpleQueue[tuple[str, DagsterEvent | Future[None]]] = SimpleQueue()
    terminal_events: dict[str, DagsterEvent] = {}
    pool = InheritContextThreadPoolExecutor(max_workers=max_concurrent)
    try:
        yield from active_execution.plan_events_iterator(job_context)
        while not active_execution.is_complete:
            if active_execution.check_for_interrupts():
                raise DagsterExecutionInterruptedError(
                    "Threaded executor was interrupted and will wait for running steps to finish."
                )

            for step in active_execution.get_steps_to_execute():
                known_state = active_execution.get_known_state()
                future = pool.submit(
                    _execute_step_on_thread,
                    job_context,
                    step,
                    known_state._replace(
                        ready_outputs=known_state.ready_outputs.copy()
                    ),
                    events,
                )
                future.add_done_callback(
                    lambda result, key=step.key: events.put((key, result))
                )

            yield from active_execution.concurrency_event_iterator(job_context)
            try:
                step_key, message = events.get(timeout=0.1)
            except Empty:
                continue

            if isinstance(message, Future):
                message.result()
                terminal_event = terminal_events.pop(step_key, None)
                if terminal_event is not None:
                    active_execution.handle_event(terminal_event)
                active_execution.verify_complete(job_context, step_key)
                yield from active_execution.plan_events_iterator(job_context)
            else:
                yield message
                if (
                    message.is_step_success
                    or message.is_step_failure
                    or message.is_step_up_for_retry
                ):
                    # A step still owns its slot until its thread has finished, including hooks.
                    terminal_events[step_key] = message
                else:
                    active_execution.handle_event(message)
    finally:
        pool.shutdown(cancel_futures=True)


def _execute_step_on_thread(
    job_context: PlanExecutionContext,
    step: ExecutionStep,
    known_state: KnownExecutionState,
    events: SimpleQueue[tuple[str, DagsterEvent | Future[None]]],
) -> None:
    with asyncio.Runner() as runner:
        # The copy keeps run-scoped resources but gives the step its own event loop.
        thread_context = copy(job_context)
        thread_context._event_loop = runner.get_loop()
        step_context = thread_context.for_step(step, known_state)
        _validate_step_resources(step_context)
        step_events = []
        for event in dagster_event_sequence_for_step(step_context):
            step_events.append(event)
            events.put((step.key, event))

        for event in _trigger_hook(step_context, step_events):
            events.put((step.key, event))


def _execute_plan(
    job_context: PlanExecutionContext,
    execution_plan: ExecutionPlan,
    *,
    max_concurrent: int,
) -> Iterator[DagsterEvent]:
    with InstanceConcurrencyContext(
        job_context.instance, job_context.dagster_run
    ) as concurrency:
        try:
            yield from _execution_iterator(
                job_context, execution_plan, concurrency, max_concurrent=max_concurrent
            )
        finally:
            # The thread pool has joined, so all of its remaining claims can be released.
            for step_key in tuple(concurrency._claims):
                concurrency.free_step(step_key)


class _ThreadedExecutor(InProcessExecutor):
    def __init__(self, retries: RetryMode, max_concurrent: int) -> None:
        super().__init__(retries=retries)
        self.max_concurrent = max_concurrent

    def execute(
        self, plan_context: PlanOrchestrationContext, execution_plan: ExecutionPlan
    ) -> Iterator[DagsterEvent]:
        yield from ExecuteRunWithPlanIterable(
            execution_plan=execution_plan,
            iterator=partial(_execute_plan, max_concurrent=self.max_concurrent),
            execution_context_manager=PlanExecutionContextManager(
                job=plan_context.job,
                retry_mode=plan_context.retry_mode,
                execution_plan=execution_plan,
                run_config=plan_context.run_config,
                dagster_run=plan_context.dagster_run,
                instance=plan_context.instance,
                raise_on_error=plan_context.raise_on_error,
                output_capture=plan_context.output_capture,
                step_dependency_config=self.step_dependency_config,
            ),
        )


@dg.executor(
    name="threaded",
    config_schema={"max_concurrent": int, "retries": get_retries_config()},
)
def threaded_executor(init_context: InitExecutorContext) -> _ThreadedExecutor:
    """Execute ready steps on threads, including synchronous ops."""
    max_concurrent = init_context.executor_config["max_concurrent"]
    check.invariant(max_concurrent > 0, "max_concurrent must be greater than zero.")
    retries = RetryMode.from_config(init_context.executor_config["retries"])
    return _ThreadedExecutor(retries, max_concurrent)

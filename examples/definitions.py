import os
import threading
import time

import dagster as dg

from dagster_threaded_executor import threaded_executor


@dg.op(out=dg.Out(dg.Nothing))
def wait_once(context: dg.OpExecutionContext):
    context.log.info(f"pid={os.getpid()} thread={threading.get_ident()}")
    time.sleep(2)


@dg.job
def two_waits():
    wait_once.alias("first")()
    wait_once.alias("second")()


defs = dg.Definitions(jobs=[two_waits], executor=threaded_executor)

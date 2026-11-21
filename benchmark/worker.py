import os

import dagster as dg
from bq_job import bigquery_job

if __name__ == "__main__":
    with dg.instance_for_test() as instance:
        result = dg.execute_job(
            dg.reconstructable(bigquery_job),
            instance=instance,
            run_config={
                "execution": {
                    "config": {"max_concurrent": int(os.environ["PROFILE_SLOTS"])}
                },
                "loggers": {"console": {"config": {"log_level": "CRITICAL"}}},
            },
        )
        if not result.success:
            for event in result.get_step_failure_events():
                print(event.step_failure_data.error.to_string())
        assert result.success
        assert sum(event.is_step_success for event in result.all_events) == int(
            os.environ["PROFILE_STEPS"]
        )

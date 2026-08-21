import random
import time

from prefect import flow
from prefect.assets import materialize
from prefect.deployments import run_deployment
from prefect.logging import get_run_logger
from prefect.runtime import flow_run

ASSET_URI = "demo://job-a/output"


@materialize(ASSET_URI)
def produce_job_a_output():
    logger = get_run_logger()
    logger.debug("job-a: starting dummy work")
    time.sleep(2)
    number = random.randint(1, 100)
    logger.info("job-a: dummy work finished, materializing %s (result=%d)", ASSET_URI, number)
    logger.warning("job-a: this is a dummy warning to show log levels in the UI")
    return number


@flow(log_prints=True)
def job_a():
    print("job-a: flow started")
    number = produce_job_a_output()

    logger = get_run_logger()
    if number > 50:
        trigger_count = (number - 50) // 10
        logger.info("job-a: result %d > 50, starting job-c %d time(s)", number, trigger_count)
        for i in range(trigger_count):
            run_deployment(
                name="job-c/job-c",
                parameters={
                    "triggered_by": f"job-a run '{flow_run.name}' ({flow_run.id}), trigger {i + 1}/{trigger_count}",
                    "random_number": number,
                },
                timeout=0,
            )
    else:
        logger.info("job-a: result %d <= 50, not starting job-c", number)

    print("job-a: flow finished")


if __name__ == "__main__":
    job_a()

import time

from prefect import flow
from prefect.assets import materialize
from prefect.logging import get_run_logger

ASSET_URI = "demo://job-b/output"


@materialize(ASSET_URI)
def produce_job_b_output():
    logger = get_run_logger()
    logger.debug("job-b: starting dummy work")
    time.sleep(2)
    logger.info("job-b: dummy work finished, materializing %s", ASSET_URI)
    logger.warning("job-b: this is a dummy warning to show log levels in the UI")


@flow(log_prints=True)
def job_b():
    print("job-b: flow started")
    produce_job_b_output()
    print("job-b: flow finished")


if __name__ == "__main__":
    job_b()

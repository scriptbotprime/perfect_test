import time

from prefect import flow
from prefect.assets import materialize
from prefect.logging import get_run_logger

ASSET_URI = "demo://job-c/output"
UPSTREAM_ASSET_URI = "demo://job-a/output"


@materialize(ASSET_URI, asset_deps=[UPSTREAM_ASSET_URI])
def produce_job_c_output():
    logger = get_run_logger()
    logger.debug("job-c: starting dummy work")
    time.sleep(2)
    logger.info("job-c: dummy work finished, materializing %s (depends on %s)", ASSET_URI, UPSTREAM_ASSET_URI)
    logger.warning("job-c: this is a dummy warning to show log levels in the UI")


@flow(log_prints=True)
def job_c(triggered_by: str = "", random_number: int | None = None):
    logger = get_run_logger()
    print(f"job-c: flow started, triggered by: {triggered_by!r}")
    logger.info("job-c: received random number from job-a: %s", random_number)
    produce_job_c_output()
    print("job-c: flow finished")


if __name__ == "__main__":
    job_c()

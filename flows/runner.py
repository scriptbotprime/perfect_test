import os

from prefect import serve

from job_a import job_a
from job_b import job_b
from job_c import job_c

deployment_a = job_a.to_deployment(name="job-a", cron="*/2 * * * *")
deployment_b = job_b.to_deployment(name="job-b", cron="*/3 * * * *")
deployment_c = job_c.to_deployment(name="job-c")

# serve() always passes its own `limit` (default None = unlimited) to the Runner,
# which overrides PREFECT_RUNNER_PROCESS_LIMIT rather than falling back to it.
# So the setting has to be read and forwarded explicitly here.
runner_limit = os.getenv("PREFECT_RUNNER_PROCESS_LIMIT")

if __name__ == "__main__":
    serve(
        deployment_a,
        deployment_b,
        deployment_c,
        limit=int(runner_limit) if runner_limit else None,
    )

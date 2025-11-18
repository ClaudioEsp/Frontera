import importlib
import logging
import signal
import time
import sys
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import JOBS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("orchestrator")


def load_job_callable(module_path: str):
    """
    Import a module and return its `run` attribute.
    module_path like 'jobs.sync_vehicles'.
    """
    module = importlib.import_module(module_path)
    if not hasattr(module, "run"):
        raise AttributeError(f"Module {module_path} has no 'run' function")
    return module.run


def schedule_jobs(scheduler: BackgroundScheduler):
    for job_cfg in JOBS:
        job_id = job_cfg["id"]
        module = job_cfg["module"]
        run_callable = load_job_callable(module)

        if job_cfg.get("type") == "interval":
            seconds = job_cfg["seconds"]
            trigger = IntervalTrigger(seconds=seconds)
            print(f"Scheduling interval job '{job_id}' every {seconds} seconds")

        else:  # default: cron
            cron_expr = job_cfg["cron"]
            trigger = CronTrigger.from_crontab(cron_expr)
            print(f"Scheduling cron job '{job_id}' with '{cron_expr}'")

        scheduler.add_job(
            func=run_callable,
            trigger=trigger,
            id=job_id,
            max_instances=1,
            coalesce=True,
        )

def main():
    scheduler = BackgroundScheduler(timezone="UTC")

    schedule_jobs(scheduler)
    scheduler.start()

    logger.info("Orchestrator started.")

    # Graceful shutdown on Ctrl+C / SIGTERM
    def shutdown(signum, frame):
        logger.info(f"Received signal {signum}, shutting down scheduler...")
        scheduler.shutdown(wait=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()

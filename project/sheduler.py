"""
Lakehouse Backup Scheduler
Automated scheduling system for lakehouse backups with consistency groups
FIXED: Proper async execution using asyncio.run
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, Dict
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import httpx
import json
from pathlib import Path
import logging
import asyncio
from contextlib import asynccontextmanager

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
ORCHESTRATOR_URL = "http://localhost:8002"
SCHEDULE_STORAGE_FILE = "backup_schedules.json"

scheduler = BackgroundScheduler()
scheduler.start()

# -------------------------------------------------
# MODELS
# -------------------------------------------------

class ScheduleConfig(BaseModel):
    schedule_id: str
    schedule_name: str
    target: str = "lakehouse"
    backup_type: str = "full"
    schedule_type: str

    cron_minute: Optional[str] = None
    cron_hour: Optional[str] = None
    cron_day: Optional[str] = None
    cron_month: Optional[str] = None
    cron_day_of_week: Optional[str] = None

    interval_minutes: Optional[int] = None
    interval_hours: Optional[int] = None
    interval_days: Optional[int] = None

    enabled: bool = True
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    run_count: int = 0


class ScheduleCreateRequest(BaseModel):
    schedule_name: str
    target: str = "lakehouse"
    backup_type: str = "full"
    schedule_type: str

    cron_minute: Optional[str] = None
    cron_hour: Optional[str] = None
    cron_day: Optional[str] = None
    cron_month: Optional[str] = None
    cron_day_of_week: Optional[str] = None

    interval_minutes: Optional[int] = None
    interval_hours: Optional[int] = None
    interval_days: Optional[int] = None

    enabled: bool = True


# -------------------------------------------------
# STORAGE
# -------------------------------------------------

class ScheduleStorage:
    def __init__(self, file: str):
        self.file = Path(file)
        self.schedules: Dict[str, ScheduleConfig] = {}
        self.load()

    def load(self):
        if self.file.exists():
            with open(self.file) as f:
                data = json.load(f)
                self.schedules = {k: ScheduleConfig(**v) for k, v in data.items()}

    def save(self):
        with open(self.file, "w") as f:
            json.dump({k: v.model_dump() for k, v in self.schedules.items()}, f, indent=2)

    def add(self, schedule: ScheduleConfig):
        self.schedules[schedule.schedule_id] = schedule
        self.save()

    def get(self, schedule_id: str):
        return self.schedules.get(schedule_id)

    def list_all(self):
        return list(self.schedules.values())


storage = ScheduleStorage(SCHEDULE_STORAGE_FILE)

# -------------------------------------------------
# BACKUP EXECUTION
# -------------------------------------------------

async def execute_backup_async(schedule: ScheduleConfig):
    logger.info(f"Executing backup: {schedule.schedule_name}")
    async with httpx.AsyncClient(timeout=120) as client:
        await client.post(
            f"{ORCHESTRATOR_URL}/backup",
            json={"target": schedule.target, "backup_type": schedule.backup_type}
        )

def execute_backup_sync(schedule: ScheduleConfig):
    asyncio.run(execute_backup_async(schedule))

# -------------------------------------------------
# SCHEDULING (FIXED)
# -------------------------------------------------

def schedule_job(schedule: ScheduleConfig):
    if not schedule.enabled:
        return

    if scheduler.get_job(schedule.schedule_id):
        scheduler.remove_job(schedule.schedule_id)

    if schedule.schedule_type == "cron":
        trigger = CronTrigger(
            minute=schedule.cron_minute or "0",
            hour=schedule.cron_hour or "*",
            day=schedule.cron_day or "*",
            month=schedule.cron_month or "*",
            day_of_week=schedule.cron_day_of_week or "*",
        )

    elif schedule.schedule_type == "interval":
        kwargs = {}

        if schedule.interval_minutes is not None:
            kwargs["minutes"] = schedule.interval_minutes
        if schedule.interval_hours is not None:
            kwargs["hours"] = schedule.interval_hours
        if schedule.interval_days is not None:
            kwargs["days"] = schedule.interval_days

        if not kwargs:
            raise ValueError("Interval schedule requires minutes, hours, or days")

        trigger = IntervalTrigger(**kwargs)

    else:
        raise ValueError("Invalid schedule_type")

    scheduler.add_job(
        lambda: execute_backup_sync(schedule),
        trigger=trigger,
        id=schedule.schedule_id,
        replace_existing=True
    )

    logger.info(f"Scheduled job: {schedule.schedule_name}")

# -------------------------------------------------
# LIFESPAN
# -------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Lakehouse Backup Scheduler...")

    count = 0
    for s in storage.list_all():
        if s.enabled:
            try:
                schedule_job(s)
                count += 1
            except Exception as e:
                logger.error(f"Failed to schedule {s.schedule_name}: {e}")

    logger.info(f"Loaded {count} active schedule(s)")
    yield
    logger.info("Shutting down scheduler...")
    scheduler.shutdown()

# -------------------------------------------------
# FASTAPI APP
# -------------------------------------------------

app = FastAPI(
    title="Lakehouse Backup Scheduler",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    return {"status": "running"}

@app.post("/schedules")
async def create_schedule(req: ScheduleCreateRequest):
    schedule_id = f"schedule_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    schedule = ScheduleConfig(schedule_id=schedule_id, **req.model_dump())
    storage.add(schedule)

    if schedule.enabled:
        schedule_job(schedule)

    return schedule.model_dump()

@app.post("/schedules/{schedule_id}/run")
async def run_now(schedule_id: str, bg: BackgroundTasks):
    schedule = storage.get(schedule_id)
    if not schedule:
        raise HTTPException(404, "Schedule not found")
    bg.add_task(execute_backup_async, schedule)
    return {"status": "triggered"}

# -------------------------------------------------
# MAIN
# -------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)

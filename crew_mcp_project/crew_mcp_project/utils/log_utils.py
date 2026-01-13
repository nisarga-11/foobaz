import logging
import os
from datetime import datetime

LOG_DIR = os.path.join(os.getcwd(), "pgbackrest", "log")
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    log_file = os.path.join(LOG_DIR, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger

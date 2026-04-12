import logging

from src.common.mysql_bootstrap import ensure_mysql_data_loaded

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    print("Starting MySQL bootstrap...")
    message = ensure_mysql_data_loaded()
    print(f"Result: {message}")

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STORAGE_DIR = BASE_DIR / "storage"
DATABASE_PATH = STORAGE_DIR / "warehouse.duckdb"

# Engine Parameters
WATERMARK_DELAY_SECONDS = 600  # 10 minutes tolerance window
DEFAULT_BATCH_SIZE = 1000
TOTAL_BATCHES_DEFAULT = 5
RANDOM_SEED = 42
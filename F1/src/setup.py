from pathlib import Path


# Global Paths consistent for each race weekend
DATA_PATH = Path("data")
DATA_PATH.mkdir(exist_ok=True)

SESSION_PATH = DATA_PATH / "session_info"
SESSION_PATH.mkdir(exist_ok=True)
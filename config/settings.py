import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    alpha_vantage_api_key: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_username: str
    clickhouse_password: str


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


SETTINGS = Settings(
    alpha_vantage_api_key=_required_env("ALPHA_VANTAGE_API_KEY"),
    clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    clickhouse_username=os.getenv("CLICKHOUSE_USERNAME", "default"),
    clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
)

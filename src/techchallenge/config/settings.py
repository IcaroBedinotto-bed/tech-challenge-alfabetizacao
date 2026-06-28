from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()


@dataclass(frozen=True)
class Settings:
    project_id: str
    dataset_id: str
    credentials_path: Path


settings = Settings(
    project_id=os.getenv("GCP_PROJECT_ID"),
    dataset_id="basedosdados.br_inep_avaliacao_alfabetizacao",
    credentials_path=Path(
        os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "credentials/google_credentials.json"
        )
    ),
)
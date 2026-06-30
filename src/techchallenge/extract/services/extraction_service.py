from pathlib import Path

import pandas as pd

from techchallenge.extract.clients.bigquery_client import BigQueryClient


class ExtractionService:

    def __init__(self):
        self.client = BigQueryClient()

    def extract_table(
        self,
        table_name: str,
        limit: int | None = None
    ) -> pd.DataFrame:

        return self.client.read_table(
            table_name=table_name,
            limit=limit
        )
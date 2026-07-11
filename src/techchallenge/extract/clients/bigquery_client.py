from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account

from techchallenge.config.settings import settings


class BigQueryClient:
    """
    Cliente responsável por acessar o Google BigQuery.
    """

    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(
            settings.credentials_path
        )

        self.client = bigquery.Client(
            project=settings.project_id,
            credentials=credentials
        )

    def read_table(
        self,
        table_name: str,
        limit: int | None = None
    ):
        query = f"""
            SELECT *
            FROM `{settings.dataset_id}.{table_name}`
        """

        if limit is not None:
            query += f"\nLIMIT {limit}"

        return self.client.query(query).to_dataframe()
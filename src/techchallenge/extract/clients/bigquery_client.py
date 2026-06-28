from google.cloud import bigquery
import pandas as pd

from techchallenge.config.settings import settings


class BigQueryClient:
    """
    Cliente responsável por acessar o Google BigQuery.
    """

    def __init__(self):
        self.client = bigquery.Client(
            project=settings.project_id
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
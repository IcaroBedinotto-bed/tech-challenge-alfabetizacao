from pathlib import Path

from techchallenge.config.bronze_tables import BRONZE_TABLES
from techchallenge.extract.services.extraction_service import ExtractionService
from techchallenge.load.parquet_writer import ParquetWriter


class BronzePipeline:

    def __init__(self):
        self.extract_service = ExtractionService()
        self.writer = ParquetWriter()

    def run(self, limit: int | None = None):

        for table in BRONZE_TABLES:

            print(f"Extraindo {table}...")

            df = self.extract_service.extract_table(
            table_name=table,
            limit=limit
        )

            self.writer.save(
                dataframe=df,
                output_path=Path(
                    f"data/bronze/{table}/{table}.parquet"
                ),
            )

            print(f"{table} concluída.")
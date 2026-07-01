from pathlib import Path
import pandas as pd
from techchallenge.config.bronze_tables import BRONZE_TABLES
from techchallenge.extract.services.extraction_service import ExtractionService
from techchallenge.load.parquet_writer import ParquetWriter
from techchallenge.transform.bronze_to_silver import bronze_to_silver


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

class SilverPipeline:

    def __init__(self):
        self.writer = ParquetWriter()

    def run(self):

        for table in BRONZE_TABLES:

            print(f"Transformando {table}...")

            bronze_path = Path(
                f"data/bronze/{table}/{table}.parquet"
            )

            dataframe = pd.read_parquet(bronze_path)

            dataframe = bronze_to_silver(dataframe)

            silver_path = Path(
                f"data/silver/{table}/{table}.parquet"
            )

            self.writer.save(
                dataframe=dataframe,
                output_path=silver_path
            )

            print(f"{table} tranformação concluída.")
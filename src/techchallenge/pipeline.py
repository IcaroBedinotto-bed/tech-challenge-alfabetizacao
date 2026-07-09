from pathlib import Path
import pandas as pd
import logging
from techchallenge.config.bronze_tables import BRONZE_TABLES
from techchallenge.extract.services.extraction_service import ExtractionService
from techchallenge.load.parquet_writer import ParquetWriter
from techchallenge.transform.bronze_to_silver import bronze_to_silver
from techchallenge.monitoring.monitor import PipelineMonitor
from techchallenge.monitoring.summary import PipelineSummary
import shutil


class BronzePipeline:

    def __init__(self):
        self.extract_service = ExtractionService()
        self.writer = ParquetWriter()

    def run(self, limit: int | None = None):
        summary = PipelineSummary()

        for table in BRONZE_TABLES:
            try:

                monitor = PipelineMonitor(
                pipeline_name="Bronze",
                table_name=table
                )

                monitor.start()
            
                print(f"Extraindo {table}...")

                bronze_path = Path(
                    f"data/bronze/batch/{table}/{table}.parquet"
                )

                df = self.extract_service.extract_table(
                    table_name=table,
                    limit=limit
                )

                self.writer.save(
                    dataframe=df,
                    output_path=Path(bronze_path),
                )

                metrics = monitor.finish(
                    input_rows=len(df),
                    output_rows=len(df),
                    columns=len(df.columns),
                    bronze_path=bronze_path,
                    silver_path=bronze_path
                )

                print(f"{table} concluída.")
                summary.add(metrics)
        
            except Exception as e:
                logging.exception(f"Erro ao processar {table}")
                print(f"Erro na tabela {table} na camada Bronze: {e}")

        summary.print()


class SilverPipeline:

    def __init__(self):
        self.writer = ParquetWriter()

    def load_bronze_table(self, table):

        batch_path = Path(
            f"data/bronze/batch/{table}/{table}.parquet"
        )

        dataframe = pd.read_parquet(batch_path)

        # Apenas a tabela alunos possui streaming
        if table != "alunos":
            return dataframe

        streaming_path = Path(
            "data/bronze/streaming/alunos"
        )

        if streaming_path.exists():

            streaming_files = list(
                streaming_path.glob("*.parquet")
            )

            if streaming_files:

                streaming_df = pd.concat(
                    [pd.read_parquet(file) for file in streaming_files],
                    ignore_index=True
                )

                dataframe = pd.concat(
                    [dataframe, streaming_df],
                    ignore_index=True
                )

        return dataframe

    def archive_streaming_files(self):

        streaming_path = Path(
            "data/bronze/streaming/alunos"
        )

        processed_path = Path(
            "data/bronze/streaming/processed"
        )

        processed_path.mkdir(
            parents=True,
            exist_ok=True
        )

        for file in streaming_path.glob("*.parquet"):

            shutil.move(
                file,
                processed_path / file.name
            )

    def run(self):

        summary = PipelineSummary()

        for table in BRONZE_TABLES:

            try:

                monitor = PipelineMonitor(
                    pipeline_name="Silver",
                    table_name=table
                )

                monitor.start()

                print(f"Transformando {table}...")

                bronze_path = Path(
                    f"data/bronze/batch/{table}/{table}.parquet"
                )

                # Toda a lógica de leitura fica aqui
                dataframe = self.load_bronze_table(table)

                input_rows = len(dataframe)

                print(
                    f"{table}: {input_rows:,} registros carregados"
                )

                dataframe = bronze_to_silver(dataframe)

                silver_path = Path(
                    f"data/silver/{table}/{table}.parquet"
                )

                self.writer.save(
                    dataframe=dataframe,
                    output_path=silver_path
                )

                if table == "alunos":
                    self.archive_streaming_files()

                metrics = monitor.finish(
                    input_rows=input_rows,
                    output_rows=len(dataframe),
                    columns=len(dataframe.columns),
                    bronze_path=bronze_path,
                    silver_path=silver_path
                )

                summary.add(metrics)

                print(f"{table} transformação concluída.")

            except Exception as e:

                logging.exception(f"Erro ao processar {table}")

                print(
                    f"Erro na tabela {table} na camada Silver: {e}"
                )

        summary.print()
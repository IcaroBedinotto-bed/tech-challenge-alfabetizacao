from pathlib import Path

from techchallenge.extract.services.extraction_service import ExtractionService
from techchallenge.load.parquet_writer import ParquetWriter


service = ExtractionService()

writer = ParquetWriter()

df = service.extract_table(
    table_name="alunos",
    limit=100
)

writer.save(
    dataframe=df,
    output_path=Path(
        "data/bronze/alunos/alunos.parquet"
    )
)

print("Arquivo salvo com sucesso!")
from techchallenge.extract.services.extraction_service import ExtractionService

service = ExtractionService()

df = service.extract_table(
    table_name="alunos",
    limit=5
)

print(df.head())
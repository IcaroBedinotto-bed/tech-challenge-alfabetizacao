from techchallenge.extract.clients.bigquery_client import BigQueryClient

client = BigQueryClient()

df = client.read_table(table_name="alunos", limit=10)

print(df)
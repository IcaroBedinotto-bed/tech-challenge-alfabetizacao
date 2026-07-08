import pandas as pd

df = pd.read_parquet("data/silver/alunos/alunos.parquet")

print("\n".join(df.columns))
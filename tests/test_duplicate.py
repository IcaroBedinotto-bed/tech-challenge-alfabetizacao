from pathlib import Path
import pandas as pd

batch = pd.read_parquet("data/bronze/batch/alunos/alunos.parquet")

streaming = pd.concat(
    [
        pd.read_parquet(f)
        for f in Path("data/bronze/streaming/alunos").glob("*.parquet")
    ],
    ignore_index=True
)

print(streaming["id_aluno"].tolist())

print(
    streaming["id_aluno"].isin(batch["id_aluno"]).sum()
)
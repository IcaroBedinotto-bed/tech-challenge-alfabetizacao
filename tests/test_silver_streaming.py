from pathlib import Path
import pandas as pd

# Bronze Batch
batch = pd.read_parquet(
    Path("data/bronze/batch/alunos/alunos.parquet")
)

# Bronze Streaming
streaming_path = Path("data/bronze/streaming/processed")

if streaming_path.exists():

    arquivos = list(streaming_path.glob("*.parquet"))

    if arquivos:
        streaming = pd.concat(
            [pd.read_parquet(f) for f in arquivos],
            ignore_index=True
        )
    else:
        streaming = pd.DataFrame()

else:
    streaming = pd.DataFrame()

# Silver
silver = pd.read_parquet(
    Path("data/silver/alunos/alunos.parquet")
)

print("=" * 40)
print(f"Batch:      {len(batch):,}")
print(f"Streaming:  {len(streaming):,}")
print(f"Esperado:   {len(batch) + len(streaming):,}")
print(f"Silver:     {len(silver):,}")
print("=" * 40)

if len(batch) + len(streaming) == len(silver):
    print("✅ Quantidade de registros está correta.")
else:
    print("❌ Quantidade diferente.")
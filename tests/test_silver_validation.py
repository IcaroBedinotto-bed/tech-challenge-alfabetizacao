from pathlib import Path

import pandas as pd


SILVER_PATH = Path("data/silver")

for arquivo in SILVER_PATH.rglob("*.parquet"):

    print("\n" + "=" * 60)
    print(f"Tabela: {arquivo.stem}")
    print("=" * 60)

    df = pd.read_parquet(arquivo)

    print(f"Linhas: {len(df)}")
    print(f"Colunas: {len(df.columns)}")

    print("\nTipos:")
    print(df.dtypes)

    print("\nValores nulos:")
    print(df.isnull().sum())

    print("\nPrimeiras linhas:")
    print(df.head(5))
from pathlib import Path

import pandas as pd

from techchallenge.transform.bronze_to_silver import bronze_to_silver


BRONZE_PATH = Path("data/bronze")


for arquivo in BRONZE_PATH.glob("*.parquet"):

    print("\n" + "=" * 60)
    print(f"Tabela: {arquivo.stem}")
    print("=" * 60)

    df = pd.read_parquet(arquivo)

    print(f"Linhas antes: {len(df)}")
    print(f"Colunas antes: {len(df.columns)}")

    df = bronze_to_silver(df)

    print(f"Linhas depois: {len(df)}")
    print(f"Colunas depois: {len(df.columns)}")

    print("\nColunas:")
    for coluna in df.columns:
        print(f" - {coluna}")

    print("\nPrimeiras linhas:")
    print(df.head(3))
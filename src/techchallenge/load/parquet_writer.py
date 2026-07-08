from pathlib import Path

import pandas as pd


class ParquetWriter:

    def save(
        self,
        dataframe: pd.DataFrame,
        output_path: Path
    ) -> None:

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        dataframe.to_parquet(
            output_path,
            index=False
        )
import pandas as pd


class Profiler:

    def profile(self, dataframe: pd.DataFrame):

        rows = len(dataframe)

        return {
            "rows": rows,
            "columns": len(dataframe.columns),
            "column_names": dataframe.columns.tolist(),
            "dtypes": dataframe.dtypes.astype(str).to_dict(),
            "nulls": dataframe.isnull().sum().to_dict(),
            "null_percentage": (
                dataframe.isnull().mean() * 100
            ).round(2).to_dict(),
            "unique_values": dataframe.nunique(dropna=False).to_dict(),
            "duplicates": int(dataframe.duplicated().sum()),
        }
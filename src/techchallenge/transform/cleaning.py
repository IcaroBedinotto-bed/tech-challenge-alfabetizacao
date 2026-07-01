import pandas as pd

def strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    string_columns = df.select_dtypes(include="object").columns

    for column in string_columns:
        df[column] = df[column].str.strip()

    return df
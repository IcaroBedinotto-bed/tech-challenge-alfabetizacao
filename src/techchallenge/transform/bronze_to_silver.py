from techchallenge.transform.cleaning import strip_string_columns
from techchallenge.transform.standardization import standardize_column_names
from techchallenge.transform.validation import remove_duplicates


def bronze_to_silver(df):

    df = standardize_column_names(df)

    df = strip_string_columns(df)

    df = remove_duplicates(df)

    return df
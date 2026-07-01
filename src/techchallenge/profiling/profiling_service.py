from pathlib import Path
import pandas as pd
from techchallenge.profiling.profiler import Profiler


class ProfilingService:

    def __init__(self):
        self.profiler = Profiler()

    def profile_bronze(self):

        bronze_path = Path("data/bronze")

        report = {}

        for parquet_file in bronze_path.rglob("*.parquet"):

            table_name = parquet_file.stem

            dataframe = pd.read_parquet(parquet_file)

            report[table_name] = self.profiler.profile(dataframe)

        return report
import pandas as pd
from techchallenge.profiling.profiler import Profiler

df = pd.read_parquet("data/bronze/alunos/alunos.parquet")

profiler = Profiler()

profile = profiler.profile(df)

print(profile)
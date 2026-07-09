from bronze_pipeline import BronzePipeline

# URLs dos arquivos Parquet que serão ingeridos para a camada Bronze
bronze_URLS = [
    "https://raw.githubusercontent.com/IcaroBedinotto-bed/tech-challenge-alfabetizacao/develop/data/bronze/alunos/alunos.parquet",
    "https://raw.githubusercontent.com/IcaroBedinotto-bed/tech-challenge-alfabetizacao/develop/data/bronze/municipio/municipio.parquet",
    "https://raw.githubusercontent.com/IcaroBedinotto-bed/tech-challenge-alfabetizacao/develop/data/bronze/uf/uf.parquet",
]



def main():
    """
    Executa a ingestão de todos os arquivos para a camada Bronze.
    """

    pipeline = BronzePipeline()

    for url in bronze_URLS:
        pipeline.execute(url)


if __name__ == "__main__":
    main()

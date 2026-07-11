from pathlib import Path

import pandas as pd

from s3_client import S3Client



class BronzePipeline:
    """
    Classe responsável pela ingestão dos arquivos da origem
    para a camada Bronze no S3.
    """

    def __init__(self):

        # Instancia o cliente responsável pelas operações no S3
        self.s3 = S3Client()

    def execute(self, github_url):
        """
        Lê um arquivo Parquet a partir de uma URL do GitHub
        e o armazena na camada Bronze.
        """

        # Obtém o nome do arquivo a partir da URL
        filename = Path(github_url).name

        # Lê o arquivo Parquet da origem
        df = pd.read_parquet(github_url)

        # Salva o DataFrame na camada Bronze
        self.s3.save_dataframe(
            dataframe=df,
            layer="bronze",
            filename=filename
        )

        print(f"{filename} enviada para camada Bronze com sucesso!")

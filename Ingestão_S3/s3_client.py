import boto3
import pandas as pd
import os

from config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_SESSION_TOKEN,
    AWS_REGION,
    BUCKET_NAME
)

class S3Client:
    """
    Classe responsável pelas operações de leitura e escrita
    de arquivos no bucket S3.
    """

    def __init__(self):

        self.bucket = BUCKET_NAME

        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=AWS_REGION
        )

    def download_file(self, layer, filename ):
        """
        Realiza o download de um arquivo do S3 para a máquina local.
        """
        self.client.download_file(
            self.bucket,
            f"{layer}/{filename}",
            filename
        )

        print(f"Download realizado: {filename}")
        return filename



    def upload_file(self, local_file, layer, filename):
        """
        Envia um arquivo local para a camada informada no bucket S3.
        """
        self.client.upload_file(
            local_file,
            self.bucket,
            f"{layer}/{filename}"
        )


    def load_dataframe(self, layer, filename):
        """
        Baixa um arquivo Parquet do S3 e o retorna como um DataFrame.
        """

        # Faz o download do arquivo
        self.download_file(layer, filename)

        # Lê o arquivo Parquet
        dataframe = pd.read_parquet(
            filename,
            engine="pyarrow"
        )

        # Remove o arquivo temporário
        os.remove(filename)

        return dataframe

    def save_dataframe(self, dataframe, layer, filename):
        """
        Salva um DataFrame em formato Parquet e realiza o upload para o S3.
        """

        # Converte o DataFrame em Parquet, para simplificar e padronizar o arquivo antes de enviar para o S3
        dataframe.to_parquet(
            filename,
            engine="pyarrow",
            index=False
        )

        # Envia o arquivo para o bucket
        self.upload_file(
            filename,
            layer,
            filename
        )

        print(f"Envio realizado: {filename}")

        # Remove o arquivo temporário
        os.remove(filename)
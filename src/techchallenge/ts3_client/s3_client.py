import boto3
import pandas as pd
import os

from techchallenge.ts3_client.config import (
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

    def list_files(self, layer):
        """
        Lê arquivos dentro de um diretório e transforma em uma lista.
        """

        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f"{layer}/"
        )

        if "Contents" not in response:
            return []

        files = []

        for obj in response["Contents"]:

            filename = obj["Key"].split("/")[-1]

            if filename:
                files.append(filename)

        return files



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

        # Calcula tamanho do arquivo em MB
        size_mb = round(
            os.path.getsize(filename) / (1024 * 1024),
            2
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

        return size_mb

    def move_file(
            self,
            source_layer,
            destination_layer,
            filename
    ):

        self.client.copy_object(
            Bucket=self.bucket,
            CopySource={
                "Bucket": self.bucket,
                "Key": f"{source_layer}/{filename}"
            },
            Key=f"{destination_layer}/{filename}"
        )

        self.client.delete_object(
            Bucket=self.bucket,
            Key=f"{source_layer}/{filename}"
        )

    def get_file_size(self, layer, filename):
        """
        Retorna o tamanho do arquivo armazenado no S3 em MB.
        """

        response = self.client.head_object(
            Bucket=self.bucket,
            Key=f"{layer}/{filename}"
        )

        size_mb = response["ContentLength"] / (1024 * 1024)

        return round(size_mb, 2)


    def get_layer_size(self, layer):

        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f"{layer}/"
        )

        total_size = 0

        for obj in response.get("Contents", []):
            total_size += obj["Size"]

        return round(total_size / (1024 * 1024), 2)
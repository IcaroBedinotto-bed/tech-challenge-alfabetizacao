from pathlib import Path
import json
import pandas as pd
from datetime import datetime
import shutil
from techchallenge.ts3_client.s3_client import S3Client

class StreamingConsumer:

    def __init__(self):

        self.input_path = Path("extract/streaming/data/temp_streaming_input")

        self.processed_path = Path("data/temp_streaming_processed")

        self.s3 = S3Client()

    def _find_json_files(self):

        return sorted(self.input_path.glob("*.json"))
    

    def _load_json_files(self, files):

        registros = []

        for file in files:

            with open(file, "r", encoding="utf-8") as f:
                registros.append(json.load(f))

        return pd.DataFrame(registros)

    
    def _move_processed_files(self, files):

        self.processed_path.mkdir(
            parents=True,
            exist_ok=True
        )

        for file in files:

            destination = self.processed_path / file.name

            shutil.move(file, destination)

    def run(self):

        print("Streaming Consumer iniciado.")


        arquivos = self._find_json_files()


        print(f"Arquivos encontrados: {len(arquivos)}")

        if not arquivos:
            print("Nenhum arquivo encontrado.")
            return

        dataframe = self._load_json_files(arquivos)

        file_name = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.s3.save_dataframe(
            dataframe=dataframe,
            layer="bronze/streaming/alunos",
            filename=f"streaming_{file_name}.parquet"
        )

        self._move_processed_files(arquivos)

        print("\nColunas:")
    

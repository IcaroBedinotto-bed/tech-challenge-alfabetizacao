import json
import random
import time
from pathlib import Path
import uuid
import os


def gerar_dados_streaming(pasta_destino: str):
    """
    Simula a chegada contínua de resultados de avaliação de alunos.
    Cada registro é salvo como um arquivo JSON.
    """

    Path(pasta_destino).mkdir(parents=True, exist_ok=True)

    print(f"🔄 Gerando dados em: {pasta_destino}")

    while True:

        id_aluno = str(uuid.uuid4())

        aluno = {
            "ano": 2024,
            "id_municipio": random.choice([3550308, 3304557, 3106200]),
            "id_escola": f"ESC-{random.randint(100,999)}",
            "id_aluno": id_aluno,
            "caderno": random.choice(["CAD-A", "CAD-B", "CAD-C"]),
            "serie": random.choice([2, 3, 5, 9]),
            "rede": random.choice(["Estadual", "Municipal", "Privada"]),
            "presenca": 1,
            "preenchimento_caderno": 1,
            "proficiencia": round(random.uniform(150, 350), 2),
            "peso_aluno": round(random.uniform(0.8, 1.2), 2)
        }

        aluno["alfabetizado"] = (
            1 if aluno["proficiencia"] >= 200 else 0
        )

        nome_arquivo = (
            Path(pasta_destino)
            / f"aluno_{id_aluno}.json"
        )

        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(
                aluno,
                f,
                ensure_ascii=False,
                indent=4
            )

        print(f"✅ {nome_arquivo.name} criado")

        time.sleep(2)


if __name__ == "__main__":

    gerar_dados_streaming(
        "data/temp_streaming_input"
    )
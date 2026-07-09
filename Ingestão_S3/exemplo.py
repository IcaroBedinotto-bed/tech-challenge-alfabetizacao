"""
Este projeto é um exemplo de utilização das funções do S3Client.
Ele pode se aplicar nos cenários: Bronze -> Silver e Silver -> Gold.
Esse código não será entregue no projeto final.
"""

# Importação da "biblioteca" S3_cliente (nome do projeto.py)
from s3_client import S3Client

# Atribuindo a uma variavel
s3 = S3Client()

# Utilizando a função de Carregar um DF. Usando a função Load.
df = s3.load_dataframe(
    "bronze", # Especificando de qual camada virá o dado
    "municipio.parquet" # Especificando o nome do arquivo (sempre em .parquet)
)

###   EXEMPLO DE TRATAMENTO PARA OUTRA CAMADA
print(df.head())

df['teste'] = "Teste Alteração"

print(df.head())

###   FINALIZAÇÃO DOS TRATAMENTOS


# Após tratamentos, salvar DF em uma nova camada, utilizando a função Save.
s3.save_dataframe(
    dataframe=df, # Especificando DF
    layer="silver", # Especificando qual a nova camada que será salvo (caso não exista, ele irá criar)
    filename="municipio.parquet" # Especificando o nome do arquivo (sempre em .parquet, Recomendado utilizar mesmo nome da camada anterior)
)





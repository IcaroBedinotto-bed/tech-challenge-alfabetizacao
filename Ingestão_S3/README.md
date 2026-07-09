# Integração com Amazon S3 utilizando Python e Boto3

## Visão Geral

Esta etapa do projeto é responsável pela comunicação entre a aplicação Python e o serviço de armazenamento **Amazon S3**.

A implementação utiliza a biblioteca **Boto3**, que é o SDK oficial da AWS para Python, permitindo realizar operações como envio, leitura e gerenciamento de arquivos armazenados em um bucket S3.

O objetivo dessa camada é disponibilizar uma interface simples para manipular arquivos dentro da AWS, abstraindo as operações de conexão e transferência de dados.

---

# Conexão com o Amazon S3

A conexão com o serviço S3 é realizada através da criação de um cliente utilizando o Boto3.

Exemplo:

```python
self.client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)
```

O método `boto3.client()` cria uma instância de comunicação com o serviço S3.

Através desse cliente, o código consegue executar operações como:

* Enviar arquivos para um bucket;
* Baixar arquivos armazenados;
* Verificar objetos existentes;
* Manipular arquivos dentro do armazenamento da AWS.

As credenciais utilizadas permitem autenticar a aplicação e autorizar o acesso aos recursos do S3.

---

# Upload de arquivos para o S3

Uma das funcionalidades implementadas é o envio de arquivos locais para o bucket S3.

O fluxo executado é:

```
Arquivo local

↓

Cliente Boto3

↓

Bucket S3
```

A função de upload recebe o caminho do arquivo local e as informações necessárias para identificar onde o arquivo será armazenado dentro do bucket.

Esse processo permite automatizar o envio de dados para a nuvem, evitando a necessidade de realizar uploads manualmente.

---

# Salvamento de DataFrames no S3

Outra funcionalidade implementada é o armazenamento de dados provenientes de DataFrames.

O processo realizado é:

1. Receber um DataFrame;
2. Converter os dados para o formato Parquet;
3. Salvar o arquivo temporariamente;
4. Enviar o arquivo gerado para o S3;
5. Remover o arquivo temporário após o upload.

Fluxo:

```
DataFrame Pandas

↓

Arquivo Parquet

↓

Upload para S3

↓

Arquivo temporário removido
```

Essa abordagem permite integrar o processamento realizado em Python com o armazenamento na AWS.

---

# Leitura de arquivos armazenados no S3

A implementação também permite recuperar arquivos armazenados no bucket.

O fluxo de leitura é:

```
Amazon S3

↓

Download do arquivo

↓

Carregamento no Python

↓

DataFrame
```

Após a leitura, os dados podem novamente ser manipulados utilizando bibliotecas como Pandas para novas etapas de processamento.

---

# Organização do código

A lógica de comunicação com o S3 foi centralizada em uma classe responsável por gerenciar as operações relacionadas ao armazenamento.

Essa organização permite:

* Reutilizar o código em diferentes partes do projeto;
* Evitar repetição de configurações;
* Facilitar manutenção;
* Separar a lógica de armazenamento das demais etapas da aplicação.

---

# Gerenciamento das credenciais AWS

Durante o desenvolvimento, as credenciais AWS foram utilizadas para autenticar a aplicação junto ao S3.

A implementação utiliza variáveis separadas:

```python
AWS_ACCESS_KEY
AWS_SECRET_KEY
```

Dessa forma, as informações de autenticação não ficam diretamente misturadas com a lógica das funções de armazenamento.

Em ambientes colaborativos, uma alternativa mais adequada seria utilizar mecanismos externos de autenticação, como variáveis de ambiente ou configurações locais da AWS, permitindo que cada integrante utilize suas próprias credenciais sem alterar o código.

---

# Resumo da implementação

A implementação desenvolvida permite que uma aplicação Python se comunique diretamente com o Amazon S3 através do Boto3.

As principais funcionalidades são:

* Criação da conexão com o S3;
* Upload de arquivos;
* Conversão e armazenamento de DataFrames em Parquet;
* Leitura de arquivos armazenados;
* Organização das operações de armazenamento em uma estrutura reutilizável.

Essa camada funciona como a integração responsável pela movimentação dos dados entre a aplicação Python e o ambiente de armazenamento na AWS.

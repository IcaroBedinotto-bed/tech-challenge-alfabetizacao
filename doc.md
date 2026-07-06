# Documentação Técnica: Pipeline de Dados Híbrido PySpark (TechChallenge - Fase 2)

## 1. Visão Geral do Projeto

Este projeto consiste em um pipeline de Engenharia de Dados de alta performance construído em **PySpark** para processar e analisar dados de alfabetização baseados nos microdados do INEP.

O sistema foi desenhado de forma híbrida (**Arquitetura Lambda/Kappa simplificada**): combina o processamento em lote (**Batch**) para dados históricos estruturados com o processamento em tempo real (**Structured Streaming**) para monitoramento de simulados estudantis. Todo o fluxo é consolidado sob os princípios da **Arquitetura Medallion** (Bronze, Silver e Gold) e práticas de **FinOps**.

---

## 2. Configuração do Ambiente e Resiliência

O pipeline conta com rotinas automatizadas de preparação de infraestrutura adequadas para rodar de forma portátil (como no Google Colab ou clusters Linux).

### 2.1. Alocação Dinâmica do Java (`JAVA_HOME`)

Para evitar falhas de diretório rígido (*hardcoded*), o script executa comandos do sistema operacional para descobrir dinamicamente onde o Java JDK está montado no ambiente atual, injetando essa configuração nas variáveis do sistema.

### 2.2. Dimensionamento de Recursos (Spark Memory Tuning)

Visando performance no processamento paralelo de arquivos compactados e fluxos contínuos, as configurações de memória foram ajustadas manualmente:

* **Spark Driver Memory:** `4g` (Garante estabilidade na orquestração e coleta de metadados).
* **Spark Executor Memory:** `4g` (Permite transformações pesadas em memória sem gerar gargalos de escrita em disco).

---

## 3. Arquitetura de Camadas (Medallion Architecture)

### 📊 Camada Bronze (Ingestão de Dados Brutos)

O motor do Spark é iniciado usando a diretriz `.master("local[*]")` para permitir o paralelismo utilizando todas as CPUs disponíveis do ambiente.

1. **Ingestão Batch (Histórico):** Os arquivos fonte originados do INEP são lidos diretamente no formato original compactado (`.csv.gz`). O Spark descompacta o fluxo de dados em memória de forma nativa durante o `spark.read`, economizando espaço em disco e banda de rede.
2. **Ciclo de Vida do Streaming (Idempotência):** Antes de iniciar a escuta contínua, o pipeline executa uma rotina de *teardown* utilizando o módulo `shutil`. Caso o diretório `bronze/streaming_alunos/` já exista, ele é completamente removido (`shutil.rmtree`) e recriado do zero. Isso garante a **idempotência do ambiente**, prevenindo que arquivos órfãos de execuções passadas causem duplicidade ou corrompam o fluxo de tempo real atual.
3. **Simulador de Eventos (Produtor Mock):** Para fins de validação do ambiente, foi desenvolvido um gerador randômico de cargas (`json` e `random`). A cada 2 segundos, um payload estruturado representando um aluno realizando um simulado é persistido em formato JSON na pasta Bronze com uma estampa temporal única (`timestamp`), imitando o comportamento de um barramento de mensageria (como Apache Kafka ou AWS Kinesis).

### ⚙️ Camada Silver (Tratamento, Tipagem e Streaming)

Os dados brutos da Camada Bronze passam por validação estrutural, higienização e regras de tolerância a falhas:

#### Fluxo Batch (INEP)

* **Casting Estrito:** Conversão das colunas chaves `ano` e `id_municipio` para o formato Inteiro (`int`), eliminando falhas de correspondência em junções.
* **Tratamento de Nulos:** Aplicação de filtros (`.isNotNull()`) na métrica principal (`taxa_alfabetizacao`), eliminando registros sem valor estatístico.
* **Formatos de Saída:** Armazenamento em arquivos colunares Parquet otimizados.

#### Fluxo Structured Streaming (Alunos)

* **Schema Enforcement (Aplicação de Esquema):** O Spark Structured Streaming exige a definição estrita de tipos para leitura. Foi implementado um objeto `StructType` (`schema_alunos`) definindo explicitamente os tipos de chaves, notas e carimbos de data/hora (`data_hora_evento`), garantindo que dados corrompidos ou fora do padrão não quebrem o pipeline.
* **Mecanismo de Escuta Contínua (`readStream`):** O Spark monitora ativamente a pasta de streaming esperando a chegada de novos arquivos JSON de forma incremental.
* **Enriquecimento Dinâmico:** Uma regra de negócio em tempo real categoriza os alunos com base na nota (`nota_simulado < 600`), criando uma coluna de `status` para acionar bandeiras de "Alerta: Abaixo da Meta" ou "Adequado".
* **Resiliência do Consumidor (Try-Catch):** Para mitigar o erro comum de concorrência no Spark (*"Query já existe"* ao reexecutar células no notebook), o início do streaming (`writeStream`) é encapsulado em uma estrutura de controle `try-except`. Se a query já estiver ativa na memória, o Spark captura a exceção de forma segura sem derrubar a sessão ativa, informando o status do processo ao desenvolvedor.
* **Escrita em Memória RAM (`memory`):** Os dados processados pelo streaming são injetados em modo `append` diretamente na memória volátil sob a tabela temporária ad-hoc `tabela_alunos_tempo_real`.
* **Consumo Analítico:** A camada de consumo expõe a tabela em memória via Spark SQL (`spark.sql("SELECT * FROM tabela_alunos_tempo_real")`), permitindo análises exploratórias imediatas e monitoramento de KPIs em tempo real com baixíssima latência.

### 🥇 Camada Gold (Modelagem Analítica - Star Schema)

A camada final consolida os dados históricos em um modelo dimensional (Estrela) otimizado para consumo por ferramentas de Business Intelligence (BI):

* **Tabela Dimensão Tempo (`dim_tempo`):** Extração de valores temporais únicos e geração de índices baseados no ano da avaliação.
* **Tabela Dimensão Rede (`dim_rede`):** Mapeamento e decodificação analítica dos códigos institucionais do INEP para nomenclatura legível em formato de texto (`2` $\rightarrow$ Estadual, `3` $\rightarrow$ Municipal, `5` $\rightarrow$ Federal/Pública).
* **Tabela Fato Alfabetização (`fato_alfabetizacao`):** Consolidação do indicador real gerado pelo município contra as metas projetadas (Anos 2024 e 2030) acopladas via `Left Join` utilizando as chaves compostas `id_municipio` e `ano`.

---

## 4. Governança e Regras de FinOps Aplicadas

Para reduzir custos operacionais de armazenamento em nuvem e poder computacional, foram adotadas as seguintes premissas:

1. **Armazenamento Colunar (Parquet):** Todo o ecossistema analítico utiliza o formato Parquet. Ele garante taxas de compressão agressivas (reduzindo em até 70% o espaço em disco comparado ao CSV bruto) e habilita a técnica de *Predicate Pushdown* (leitura seletiva de colunas).
2. **Particionamento Estratégico:** A tabela fato foi gravada com a partição física estruturada por tempo: `.partitionBy("id_tempo")`. Isso força o particionamento do diretório de armazenamento. Quando uma ferramenta de BI ou analista filtrar por um ano específico, o motor lerá exclusivamente a pasta correspondente, evitando leituras completas (*Full Table Scans*) e reduzindo drasticamente os custos de processamento de queries.

---

# Pipeline Híbrido para Análise da Alfabetização no Brasil

Este repositório contém a solução desenvolvida para o **Tech Challenge - Fase 2**, que consiste em um pipeline híbrido de dados (Batch e Streaming) estruturado sob a Arquitetura Medalhão para integrar e analisar dados públicos sobre a alfabetização infantil no Brasil.

---

## 📋 Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico de um país. Com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental, foi instituído o **Compromisso Nacional Criança Alfabetizada**, uma política pública que mobiliza a União, estados, Distrito Federal e municípios.

Para apoiar e subsidiar essa iniciativa com parâmetros nacionais, o INEP realizou a **Pesquisa Alfabetiza Brasil** em 2023. O grande desafio para gestores públicos e cientistas de dados é que compreender e mitigar as desigualdades educacionais exige ir além da análise isolada de indicadores. É fundamental cruzar dados macro de metas nacionais com realidades territoriais e microdados educacionais altamente heterogêneos para viabilizar políticas públicas baseadas em evidências.

---

## 🎓 Desafio Educacional e Indicador de Alfabetização

A partir da Pesquisa Alfabetiza Brasil, determinou-se um ponto de corte técnico de **743 pontos** na escala de proficiência do Saeb, estabelecendo o nível mínimo a partir do qual uma criança é considerada formalmente alfabetizada. 

Com base nesse corte, instituiu-se o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar de proficiência. O desafio educacional deste projeto centra-se em consolidar e monitorar este indicador frente às metas estabelecidas, as quais preveem que 100% das crianças atinjam a meta até 2030. A engenharia de dados atua aqui como o motor que viabiliza o cruzamento do indicador de alfabetização com dados de UF, municípios e dados de alunos para identificar gargalos geográficos e operacionais.

---

## 🏛️ Arquitetura Proposta

A arquitetura foi projetada em ambiente de nuvem (**[AWS / GCP / Azure]**) seguindo o paradigma híbrido (*Lambda Architecture*) para lidar eficientemente com duas velocidades de dados:

1. **Camada Batch (Lote):** Processamento periódico focado em dados históricos e volumosos (Metas nacionais, estaduais e municipais, além de dados territoriais fornecidos pela plataforma *Base dos Dados*).
2. **Camada Streaming (Tempo Real):** Simulação de eventos em tempo quase real para capturar atualizações dinâmicas de indicadores, novas medições de desempenho escolar e alterações imediatas de resultados.

Para garantir a qualidade e governança, os dados progridem através da **Arquitetura Medalhão** (camadas Bronze, Silver e Gold) antes de estarem prontos para o consumo.

---

## ⚙️ Descrição da Arquitetura da Solução

O ecossistema de dados está dividido em quatro etapas funcionais de processamento:

* **Ingestão:** Os dados históricos da *Base dos Dados* são extraídos via rotinas Batch. Paralelamente, eventos contínuos alimentam uma fila de mensagens em tempo real (Streaming).
* **Camada Bronze (Raw Data):** Armazenamento de persistência de dados brutos exatamente como vieram da fonte. O histórico completo é preservado em formato imutável.
* **Camada Silver (Dados Tratados):** Camada onde ocorrem as transformações pesadas: limpeza de dados, tratamento de valores ausentes, padronização de esquemas/tipos, validação de chaves e a unificação/enriquecimento de dados de diferentes tabelas heterogêneas.
* **Camada Gold (Camada Analítica):** Tabelas agregadas e otimizadas em formato colunar altamente performático. Esta camada expõe visões prontas como a *evolução temporal do indicador de alfabetização por município* e a *comparação direta entre metas esperadas e resultados atingidos*.

---

## 🗺️ Diagrama da Pipeline

O fluxo visual abaixo exemplifica como os dados transitam entre as tecnologias e as camadas da arquitetura medalhão:


```

[Fontes Batch: Base dos Dados] ───> [Processo Batch] ──────┐
│
[Fontes Streaming: Eventos]   ───> [Mensageria/Fila] ──────┴─> [Camada BRONZE (S3 / Cloud Storage)]
│
[Job de Tratamento]
│
▼
[Camada SILVER (Parquet)]
│
[Agregações e Negócio]
│
▼
[Camada GOLD (Datamarts)]
│
┌───────────────────────────┴───────────────────────────┐
▼                                                       ▼
[Dashboards BI / Analytics]                             [Modelos IA / Analytics]

```

---

## 🔄 Fluxo de Dados

[cite_start]O ciclo de vida do dado dentro da pipeline cumpre o seguinte fluxo lógico[cite: 148]:

1. [cite_start]**Coleta e Origem:** Ingestão dos datasets de UF, Municípios, Dados de Alunos e tabelas de Metas a partir do repositório público[cite: 51, 52, 53, 54, 55, 56, 57].
2. [cite_start]**Carga Inicial (Bronze):** Os arquivos são gravados no *Storage* mantendo sua estrutura nativa (ex: JSON ou CSV)[cite: 77].
3. [cite_start]**Qualidade e Integração (Silver):** É executado um pipeline que valida regras estritas de qualidade (deduplicação, checagem de nulos e integridade referencial entre os códigos de municípios)[cite: 81, 82, 111, 112, 113, 114]. [cite_start]O resultado é persistido em formato colunar (ex: **Parquet**) otimizado para consultas[cite: 128].
4. [cite_start]**Consumo Analítico (Gold):** Cruzamento final gerando métricas agregadas por ano, região e rede de ensino, prontas para alimentar dashboards e ferramentas de decisão governamental[cite: 87, 91, 93].

---

## 🛠️ Tecnologias Utilizadas e Decisões Arquiteturais

### Ferramentas Escolhidas e Justificativas
* [cite_start]**Provedor de Nuvem:** `[AWS / GCP / Azure]` devido à maturidade de infraestrutura elástica e ferramentas nativas de Big Data[cite: 136, 151].
* [cite_start]**Processamento Spark / Engine:** `[Databricks / AWS Glue / EM R]` para garantir escalabilidade horizontal no tratamento de grandes volumes de microdados educacionais[cite: 21, 29, 151].
* [cite_start]**Armazenamento:** `[Amazon S3 / Google Cloud Storage]` pelo excelente custo-benefício de retenção a longo prazo (FinOps)[cite: 39, 128, 151].
* [cite_start]**Streaming:** `[Kafka / AWS Kinesis / GCP PubSub]` para garantir baixa latência na captura de novos eventos de proficiência[cite: 68, 151].

### Trade-offs Considerados
* [cite_start]**Batch vs. Streaming:** Optou-se por uma pipeline híbrida porque os dados agregados do Saeb mudam anualmente (Batch), mas as atualizações das medições locais de desempenho nas escolas exigem respostas em tempo quase real (Streaming) para intervenções pedagógicas rápidas[cite: 12, 63, 68, 155].
* [cite_start]**Data Lake vs. Data Warehouse:** A utilização da arquitetura Lakehouse (armazenamento em arquivos estruturados como Parquet) foi escolhida em vez de um DW tradicional para manter os custos operacionais baixos, preservando a capacidade de escalar para análise massiva de dados não estruturados[cite: 128, 156].

---

## 📉 Monitoramento e FinOps (Otimização de Custos)

* [cite_start]**Observabilidade:** O pipeline conta com mecanismos de monitoramento para rastrear falhas de ingestão, latência de ponta a ponta e volume de dados processados através de alertas e métricas centralizadas[cite: 117, 119, 120, 121, 122].
* [cite_start]**Práticas de FinOps:** * Uso mandatório de arquivos compactados em formato **Parquet** com estratégias de particionamento por Ano e UF, reduzindo drasticamente o volume de dados varridos por query[cite: 128, 129].
    * [cite_start]Políticas de ciclo de vida (*Lifecycle*) configuradas para mover dados históricos frios da camada Bronze para classes de armazenamento mais baratas[cite: 79, 134].

---

## 🤖 Aplicação em Inteligência Artificial (IA)

[cite_start]A base estruturada na camada **Gold** serve como insumo ideal e confiável para iniciativas avançadas de Ciência de Dados e IA[cite: 87, 163]:
* [cite_start]**Modelos de Predição de Alfabetização:** Criação de modelos preditivos (como regressões ou árvores de decisão) para antecipar quais municípios correm risco de não atingirem a meta de 2030[cite: 14, 100, 164].
* [cite_start]**Análise de Desigualdade e Vulnerabilidade:** Enriquecimento futuro utilizando dados socioeconômicos do IBGE e do Censo Escolar para clusterizar municípios com perfis de vulnerabilidade educacional semelhantes, permitindo a distribuição otimizada de recursos públicos[cite: 24, 99, 100, 103, 104, 165].

```

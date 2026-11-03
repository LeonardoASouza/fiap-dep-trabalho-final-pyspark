# Relatório de Pedidos Recusados e Legítimos

Projeto PySpark desenvolvido como **Trabalho Final** da disciplina
**Data Engineering Programming** — Prof. Marcelo Barbosa Pinto.

---

## 1. Escopo de negócio

A alta gestão da empresa solicitou um relatório dos **pedidos de venda cujo
pagamento foi RECUSADO** (`status = false`) e que, **na avaliação de fraude,
foram classificados como LEGÍTIMOS** (`avaliacao_fraude.fraude = false`).

| # | Atributo do relatório | Coluna gerada        | Origem                                             |
|---|-----------------------|----------------------|----------------------------------------------------|
| 1 | Identificador do pedido | `id_pedido`        | `pedidos.id_pedido`                                 |
| 2 | Estado (UF)             | `uf`               | `pedidos.uf`                                        |
| 3 | Forma de pagamento      | `forma_pagamento`  | `pagamentos.forma_pagamento`                        |
| 4 | Valor total do pedido   | `valor_total_pedido` | `pedidos.valor_unitario * pedidos.quantidade`     |
| 5 | Data do pedido          | `data_pedido`      | `pedidos.data_criacao`                              |

Regras aplicadas:

- somente pedidos **do ano de 2025** (`year(data_criacao) = 2025`);
- ordenação por **UF**, **forma de pagamento** e **data de criação do pedido**
  (com `id_pedido` como desempate, para que a saída seja determinística);
- gravação do resultado em **formato parquet**.

---

## 2. Arquitetura

O projeto segue os princípios de engenharia de software aplicados a PySpark:
**orientação a objetos**, **injeção de dependências** e **separação de
responsabilidades em pacotes**.

```
                          ┌──────────────────────────┐
                          │         main.py          │
                          │  (Aggregation Root /     │
                          │   Composition Root)      │
                          └────────────┬─────────────┘
         instancia e injeta            │
   ┌────────────┬────────────┬─────────┴────┬──────────────┬────────────────┐
   │            │            │              │              │                │
┌──▼───────┐ ┌──▼─────────┐ ┌▼───────────┐ ┌▼───────────┐ ┌▼──────────────┐
│AppConfig │ │SparkSession│ │SchemaCatalog│ │DataHandler │ │ Transformer   │
│ (config) │ │  Manager   │ │  (schemas)  │ │ (io_utils) │ │ (processing)  │
└──────────┘ └────────────┘ └─────────────┘ └────────────┘ └───────────────┘
                                   └──────────┬──────────────────┘
                                        ┌─────▼──────┐
                                        │  Pipeline  │
                                        │ (pipeline) │
                                        └────────────┘
```

| Pacote                        | Classe                                   | Responsabilidade                                  |
|-------------------------------|------------------------------------------|---------------------------------------------------|
| `pedidos_recusados.config`    | `AppConfig`                              | Configurações centralizadas (YAML)                |
| `pedidos_recusados.session`   | `SparkSessionManager`                    | Ciclo de vida da `SparkSession`                   |
| `pedidos_recusados.schemas`   | `SchemaCatalog`                          | Schemas **explícitos** de todos os dataframes     |
| `pedidos_recusados.io_utils`  | `DataHandler`                            | Leitura (CSV/JSON) e escrita (parquet)            |
| `pedidos_recusados.processing`| `RelatorioPedidosRecusadosTransformer`   | Regras de negócio, logging e tratamento de erros  |
| `pedidos_recusados.pipeline`  | `Pipeline`                               | Orquestração das etapas do pipeline               |

Nenhuma classe instancia suas próprias dependências: todas as recebem prontas
no construtor. O único ponto do projeto que monta o grafo de objetos é o
`main.py`.

### Estrutura de diretórios

```
trabalho-final-pyspark/
├── config/
│   ├── settings.yaml              # configuração centralizada da aplicação
│   └── log4j2.properties          # (opcional) reduz o ruído dos logs do Spark
├── data/
│   ├── input/                     # datasets baixados (não versionados)
│   └── output/                    # relatório em parquet (não versionado)
├── scripts/
│   └── download-datasets.sh       # baixa os datasets oficiais do trabalho
├── src/
│   └── pedidos_recusados/
│       ├── config/settings.py
│       ├── io_utils/data_handler.py
│       ├── io_utils/exceptions.py
│       ├── pipeline/pipeline.py
│       ├── processing/transformations.py
│       ├── schemas/catalog.py
│       ├── session/spark_session.py
│       └── main.py                # Aggregation Root
├── tests/
│   ├── conftest.py
│   ├── unit/
│   └── integration/
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 3. Pré-requisitos

| Item   | Versão              |
|--------|---------------------|
| Python | 3.10 ou superior    |
| Java   | **17 ou superior** (exigido pelo Spark 4.x) |
| Git    | qualquer versão recente |

```bash
python3 --version
java -version
```

---

## 4. Instalação

O projeto roda em qualquer um dos três ambientes abaixo. Escolha **um**.

### Opção A — GitHub Codespaces (recomendada)

O repositório traz um **dev container** (`.devcontainer/`) com Python 3.11 e
Java 17 já configurados. Nada precisa ser instalado na máquina local.

1. No GitHub, abra o repositório → botão **Code** → aba **Codespaces** →
   **Create codespace on main**.
2. Aguarde a construção do container. O `postCreateCommand` executa
   automaticamente `.devcontainer/setup.sh`, que instala as dependências,
   instala o pacote em modo editável e baixa os datasets.
3. Quando o terminal exibir `>> Ambiente pronto.`, siga para a seção 5.

Se preferir rodar o dev container localmente: VS Code + extensão
**Dev Containers** → *Reopen in Container*.

### Opção B — AWS Academy Learner Lab (EC2 / terminal do laboratório)

1. Inicie o laboratório (**Start Lab**) e aguarde o indicador **AWS** ficar
   verde.
2. No terminal do laboratório (ou via SSH em uma instância EC2 Ubuntu):

```bash
sudo apt-get update -y
sudo apt-get install -y openjdk-17-jdk python3-venv git
java -version

git clone https://github.com/<SEU-USUARIO>/<SEU-REPOSITORIO>.git
cd <SEU-REPOSITORIO>

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .

bash scripts/download-datasets.sh
```

> O laboratório expira em 4 horas e tem orçamento limitado; encerre com
> **End Lab** ao terminar.

### Opção C — Máquina local

**Linux / macOS**

```bash
git clone https://github.com/<SEU-USUARIO>/<SEU-REPOSITORIO>.git
cd <SEU-REPOSITORIO>
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
bash scripts/download-datasets.sh
```

**Windows (PowerShell)** — requer JDK 17 instalado (ex.: Temurin 17):

```powershell
git clone https://github.com/<SEU-USUARIO>/<SEU-REPOSITORIO>.git
cd <SEU-REPOSITORIO>
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
bash scripts/download-datasets.sh    # Git Bash ou WSL
```

> `pip install -e .` instala o pacote `pedidos_recusados` em modo editável,
> deixando-o importável a partir de qualquer diretório.

### Datasets

O script `scripts/download-datasets.sh` clona os repositórios oficiais e popula:

- `data/input/pedidos`    — `pedidos-YYYY-MM.csv.gz`  (CSV, separador `;`, gzip)
- `data/input/pagamentos` — `pagamentos-YYYY-MM.json.gz` (JSON lines, gzip)

**Alternativa manual (qualquer sistema operacional):**

```bash
git clone --depth 1 https://github.com/infobarbosa/datasets-csv-pedidos.git
git clone --depth 1 https://github.com/infobarbosa/dataset-json-pagamentos.git
# copie datasets-csv-pedidos/data/pedidos       -> data/input/pedidos
# copie dataset-json-pagamentos/data/pagamentos -> data/input/pagamentos
```

---

## 5. Execução

Três formas equivalentes, a partir da raiz do projeto:

```bash
# 1) via spark-submit (forma recomendada)
spark-submit src/pedidos_recusados/main.py

# 2) via módulo Python
python -m pedidos_recusados.main

# 3) via entry point instalado pelo pacote
run-pedidos-recusados
```

Para uma saída de console limpa (apenas os logs da aplicação):

```bash
spark-submit \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=config/log4j2.properties" \
  src/pedidos_recusados/main.py
```

Para usar outro arquivo de configuração:

```bash
spark-submit src/pedidos_recusados/main.py /caminho/para/settings.yaml
# ou
export PEDIDOS_RECUSADOS_CONFIG=/caminho/para/settings.yaml
```

### Saída esperada

```
2026-11-02 19:24:16 - INFO - Relatorio gerado com 1473 pedidos
+------------------------------------+---+---------------+------------------+-----------+
|id_pedido                           |uf |forma_pagamento|valor_total_pedido|data_pedido|
+------------------------------------+---+---------------+------------------+-----------+
|acbf9b42-eaca-4cd9-914a-109de59aa943|AL |BOLETO         |2500.0            |2025-02-02 |
|df280f1a-b8f4-4a29-be64-66fb7fab2a25|AL |BOLETO         |1100.0            |2025-06-12 |
|394c5640-fae2-413f-bdfb-52f316fa6782|AL |CARTAO_CREDITO |5000.0            |2025-01-03 |
+------------------------------------+---+---------------+------------------+-----------+
```

O relatório é gravado em
`data/output/relatorio_pedidos_recusados_legitimos/` (parquet + `_SUCCESS`).

### Conferência do resultado

```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('conferencia').getOrCreate()
df = spark.read.parquet('data/output/relatorio_pedidos_recusados_legitimos')
df.printSchema(); print('Total de linhas:', df.count()); df.show(20, truncate=False)
"
```

---

## 6. Configuração

Todos os parâmetros ficam em `config/settings.yaml` — nenhum caminho ou regra
está embutido no código:

```yaml
spark:
  app_name: "Relatorio de Pedidos Recusados e Legitimos"
  master: "local[*]"
paths:
  pedidos: "data/input/pedidos"
  pagamentos: "data/input/pagamentos"
  output: "data/output/relatorio_pedidos_recusados_legitimos"
business_rules:
  ano_referencia: 2025
  status_pagamento: false
  fraude: false
```

Caminhos relativos são resolvidos a partir da raiz do projeto; caminhos
absolutos e URIs (`s3://`, `hdfs://`, `dbfs:/`) são usados como vieram — é o
que mantém o projeto **agnóstico à plataforma** de execução (local, EMR,
Databricks, Kubernetes).

---

## 7. Testes

```bash
pytest                      # suíte completa
pytest -m unit              # apenas testes unitários
pytest -m integration       # apenas testes de integração
pytest --cov=src --cov-report=term-missing
```

A suíte cobre a classe de lógica de negócio (filtro por ano, cálculo do valor
total, filtro de pagamentos recusados/legítimos, ordenação, validação de schema
e tratamento de erro), a camada de I/O, a configuração, o gerenciador de sessão
e o pipeline completo com dependências injetadas.

---

## 8. Empacotamento

```bash
python -m build
```

Gera `dist/pedidos_recusados-1.0.0-py3-none-any.whl` e o `.tar.gz`.
Para submeter em um cluster:

```bash
spark-submit \
  --py-files dist/pedidos_recusados-1.0.0-py3-none-any.whl \
  --files config/settings.yaml \
  src/pedidos_recusados/main.py
```

---

## 9. Qualidade de código

```bash
ruff check src tests
black --check src tests
```

---

## 10. Datasets

| Dataset    | Formato | Repositório |
|------------|---------|-------------|
| Pedidos    | CSV (`;`, gzip)  | https://github.com/infobarbosa/datasets-csv-pedidos |
| Pagamentos | JSON lines (gzip) | https://github.com/infobarbosa/dataset-json-pagamentos |

Material de apoio da disciplina: https://github.com/infobarbosa/pyspark-poo

---

## 11. Autores

MBA FIAP — Data Engineering Programming — Prof. Marcelo Barbosa Pinto

| Nome | RM |
|------|----|
| Arthur Oliveira Araújo | 373229 |
| Helen Jerônimo Martins | 373091 |
| Joyce Paduan de Souza Maciel | 374583 |
| Julio Mateus Costa da Silva | 376632 |
| Leonardo de Almeida Souza | 374284 |

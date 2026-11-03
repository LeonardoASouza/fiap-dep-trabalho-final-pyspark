#!/usr/bin/env bash
# =============================================================================
# Preparacao automatica do ambiente (GitHub Codespaces / Dev Container).
#
# Executado uma unica vez, logo apos a criacao do container:
#   1. instala as dependencias de execucao, teste e empacotamento;
#   2. instala o pacote do projeto em modo editavel;
#   3. baixa os datasets oficiais do trabalho.
# =============================================================================
set -euo pipefail

echo ">> Java disponivel:"
java -version

echo ">> Instalando dependencias..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .

echo ">> Baixando os datasets..."
bash scripts/download-datasets.sh

echo ">> Ambiente pronto. Para executar:"
echo "   spark-submit --conf \"spark.driver.extraJavaOptions=-Dlog4j.configurationFile=config/log4j2.properties\" src/pedidos_recusados/main.py"

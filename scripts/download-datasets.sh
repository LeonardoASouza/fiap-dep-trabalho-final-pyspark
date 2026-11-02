#!/usr/bin/env bash
# =============================================================================
# Baixa os datasets oficiais do trabalho para ./data/input
#
#   - Pedidos   (CSV  ; gzip) -> data/input/pedidos
#   - Pagamentos(JSON ; gzip) -> data/input/pagamentos
#
# Uso:  bash scripts/download-datasets.sh
# =============================================================================
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${PROJECT_ROOT}/data/input"
TMP_DIR="$(mktemp -d)"

trap 'rm -rf "${TMP_DIR}"' EXIT

echo ">> Diretorio de destino: ${INPUT_DIR}"
mkdir -p "${INPUT_DIR}"

echo ">> Clonando o dataset de pedidos..."
git clone --depth 1 https://github.com/infobarbosa/datasets-csv-pedidos.git "${TMP_DIR}/pedidos"

echo ">> Clonando o dataset de pagamentos..."
git clone --depth 1 https://github.com/infobarbosa/dataset-json-pagamentos.git "${TMP_DIR}/pagamentos"

echo ">> Copiando os arquivos..."
rm -rf "${INPUT_DIR}/pedidos" "${INPUT_DIR}/pagamentos"
cp -r "${TMP_DIR}/pedidos/data/pedidos" "${INPUT_DIR}/pedidos"
cp -r "${TMP_DIR}/pagamentos/data/pagamentos" "${INPUT_DIR}/pagamentos"

echo ">> Datasets disponiveis:"
echo "   - $(ls -1 "${INPUT_DIR}/pedidos" | wc -l) arquivo(s) em data/input/pedidos"
echo "   - $(ls -1 "${INPUT_DIR}/pagamentos" | wc -l) arquivo(s) em data/input/pagamentos"
echo ">> Concluido."

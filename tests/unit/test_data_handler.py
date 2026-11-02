"""Testes unitarios da camada de leitura e escrita (I/O)."""

from __future__ import annotations

import gzip
import json

import pytest

from pedidos_recusados.io_utils.data_handler import DataHandler
from pedidos_recusados.io_utils.exceptions import DataReadError

pytestmark = pytest.mark.unit


@pytest.fixture
def data_handler(spark, schemas) -> DataHandler:
    return DataHandler(spark=spark, schemas=schemas)


@pytest.fixture
def pedidos_csv(tmp_path):
    destino = tmp_path / "pedidos"
    destino.mkdir()
    conteudo = (
        "ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE\n"
        "p1;NOTEBOOK;1500.0;2;2025-03-10T13:30:49;SP;1\n"
        "p2;MONITOR;600.0;3;2025-07-20T18:45:00;RJ;2\n"
    )
    with gzip.open(destino / "pedidos-2025-01.csv.gz", "wt", encoding="utf-8") as handler:
        handler.write(conteudo)
    return str(destino)


@pytest.fixture
def pagamentos_json(tmp_path):
    destino = tmp_path / "pagamentos"
    destino.mkdir()
    registros = [
        {
            "id_pedido": "p1",
            "forma_pagamento": "PIX",
            "valor_pagamento": 3000.0,
            "status": False,
            "data_processamento": "2025-03-10T14:00:00.123456",
            "avaliacao_fraude": {"fraude": False, "score": 0.2},
        }
    ]
    with gzip.open(destino / "pagamentos-2025-01.json.gz", "wt", encoding="utf-8") as handler:
        for registro in registros:
            handler.write(json.dumps(registro) + "\n")
    return str(destino)


def test_read_pedidos_aplica_o_schema_explicito(data_handler, schemas, pedidos_csv):
    df = data_handler.read_pedidos(pedidos_csv, {"compression": "gzip", "header": True, "sep": ";"})

    esperado = [(f.name, f.dataType.simpleString()) for f in schemas.pedidos().fields]
    obtido = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]

    assert obtido == esperado
    assert df.count() == 2


def test_read_pagamentos_le_o_objeto_aninhado_de_fraude(data_handler, pagamentos_json):
    df = data_handler.read_pagamentos(
        pagamentos_json,
        {"compression": "gzip", "timestamp_format": "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSS]"},
    )

    linha = df.collect()[0]
    assert linha.status is False
    assert linha.avaliacao_fraude.fraude is False
    assert linha.data_processamento is not None


def test_read_pedidos_lanca_erro_para_caminho_inexistente(data_handler, tmp_path):
    with pytest.raises(DataReadError):
        data_handler.read_pedidos(str(tmp_path / "nao-existe"))


def test_write_parquet_grava_e_le_de_volta(data_handler, schemas, spark, tmp_path):
    origem = spark.createDataFrame(
        [("p1", "SP", "PIX", 3000.0, __import__("datetime").date(2025, 3, 10))],
        schema=schemas.relatorio(),
    )
    destino = str(tmp_path / "saida")

    data_handler.write_parquet(origem, destino, {"mode": "overwrite", "coalesce": 1})
    lido = data_handler.read_parquet(destino)

    assert lido.count() == 1
    assert lido.columns == schemas.relatorio_columns()

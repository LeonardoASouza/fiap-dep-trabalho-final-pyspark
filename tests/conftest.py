"""Fixtures compartilhadas por toda a suite de testes."""

from __future__ import annotations

import datetime

import pytest
from pyspark.sql import SparkSession

from pedidos_recusados.schemas.catalog import SchemaCatalog


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession unica, reutilizada por toda a suite (criar sessao e caro)."""
    session = (
        SparkSession.builder.appName("test-pedidos-recusados")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def schemas() -> SchemaCatalog:
    """Catalogo de schemas explicitos injetado nos componentes sob teste."""
    return SchemaCatalog()


@pytest.fixture
def pedidos_df(spark, schemas):
    """Pedidos de teste: 3 de 2025 e 1 de 2024 (fora do escopo do relatorio)."""
    linhas = [
        ("p1", "NOTEBOOK", 1500.0, 2, datetime.datetime(2025, 3, 10, 13, 30, 49), "SP", 1),
        ("p2", "CELULAR", 1000.0, 1, datetime.datetime(2025, 1, 5, 9, 0, 0), "SP", 2),
        ("p3", "MONITOR", 600.0, 3, datetime.datetime(2025, 7, 20, 18, 45, 0), "RJ", 3),
        ("p4", "GELADEIRA", 2000.0, 1, datetime.datetime(2024, 6, 15, 12, 0, 0), "MG", 4),
    ]
    return spark.createDataFrame(linhas, schema=schemas.pedidos())


@pytest.fixture
def pagamentos_df(spark, schemas):
    """Pagamentos de teste cobrindo as quatro combinacoes de status x fraude."""
    linhas = [
        # recusado + legitimo  -> ENTRA no relatorio
        ("p1", "PIX", 3000.0, False, datetime.datetime(2025, 3, 10, 14, 0, 0), (False, 0.20)),
        # recusado + legitimo  -> ENTRA no relatorio
        ("p2", "BOLETO", 1000.0, False, datetime.datetime(2025, 1, 5, 10, 0, 0), (False, 0.11)),
        # recusado + fraude    -> NAO entra
        ("p3", "PIX", 1800.0, False, datetime.datetime(2025, 7, 20, 19, 0, 0), (True, 0.99)),
        # aprovado + legitimo  -> NAO entra (e o pedido e de 2024)
        (
            "p4",
            "CARTAO_CREDITO",
            2000.0,
            True,
            datetime.datetime(2024, 6, 15, 13, 0, 0),
            (False, 0.05),
        ),
    ]
    return spark.createDataFrame(linhas, schema=schemas.pagamentos())

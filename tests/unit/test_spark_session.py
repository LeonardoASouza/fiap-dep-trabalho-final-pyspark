"""Testes unitarios do gerenciador de sessao Spark."""

from __future__ import annotations

import pytest

from pedidos_recusados.session.spark_session import SparkSessionManager

pytestmark = pytest.mark.unit


def test_get_or_create_devolve_sempre_a_mesma_sessao(spark):
    manager = SparkSessionManager(app_name="teste-sessao", master="local[1]")

    primeira = manager.get_or_create()
    segunda = manager.get_or_create()

    assert primeira is segunda


def test_session_e_none_antes_da_criacao():
    manager = SparkSessionManager(app_name="teste-sessao-lazy", master="local[1]")

    assert manager.session is None


def test_configs_sao_aplicadas_na_sessao(spark):
    manager = SparkSessionManager(
        app_name="teste-config",
        master="local[1]",
        configs={"spark.sql.session.timeZone": "UTC"},
    )

    sessao = manager.get_or_create()

    assert sessao.conf.get("spark.sql.session.timeZone") == "UTC"

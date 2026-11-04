"""Testes unitarios do gerenciador de sessao Spark.

A classe e testada em isolamento, com um construtor (``builder``) dublê.
Testar contra uma ``SparkSession`` real nao funcionaria: o
``SparkSession.builder.getOrCreate()`` devolve sempre a sessao global ativa,
entao os testes passariam mesmo que a classe nao fizesse nada.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pedidos_recusados.session.spark_session import SparkSessionManager

pytestmark = pytest.mark.unit


@pytest.fixture
def builder_duble():
    """Builder encadeavel falso, que devolve uma sessao falsa no fim."""
    builder = MagicMock(name="builder")
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder

    sessao = MagicMock(name="sessao")
    sessao.version = "4.2.1"
    sessao.sparkContext.master = "local[1]"
    builder.getOrCreate.return_value = sessao

    with patch("pedidos_recusados.session.spark_session.SparkSession") as spark_falso:
        spark_falso.builder = builder
        yield builder, sessao


def test_session_e_none_antes_da_criacao():
    manager = SparkSessionManager(app_name="teste-lazy", master="local[1]")

    assert manager.session is None


def test_get_or_create_cria_a_sessao_uma_unica_vez(builder_duble):
    builder, sessao = builder_duble
    manager = SparkSessionManager(app_name="teste-cache", master="local[1]")

    primeira = manager.get_or_create()
    segunda = manager.get_or_create()

    assert primeira is sessao
    assert segunda is sessao
    # A segunda chamada reaproveita a sessao em vez de construir outra.
    assert builder.getOrCreate.call_count == 1


def test_app_name_e_repassado_ao_builder(builder_duble):
    builder, _ = builder_duble

    SparkSessionManager(app_name="Relatorio de Pedidos", master="local[1]").get_or_create()

    builder.appName.assert_called_once_with("Relatorio de Pedidos")


def test_configs_sao_repassadas_ao_builder(builder_duble):
    builder, _ = builder_duble
    configs = {"spark.sql.shuffle.partitions": "8", "spark.sql.session.timeZone": "UTC"}

    SparkSessionManager(app_name="teste", master="local[1]", configs=configs).get_or_create()

    builder.config.assert_any_call("spark.sql.shuffle.partitions", "8")
    builder.config.assert_any_call("spark.sql.session.timeZone", "UTC")
    assert builder.config.call_count == 2


def test_master_e_aplicado_quando_informado(builder_duble):
    builder, _ = builder_duble

    SparkSessionManager(app_name="teste", master="local[4]").get_or_create()

    builder.master.assert_called_once_with("local[4]")


def test_master_nao_e_aplicado_quando_ausente(builder_duble):
    """Sem master na configuracao, respeita o que vier do spark-submit."""
    builder, _ = builder_duble

    SparkSessionManager(app_name="teste", master=None).get_or_create()

    builder.master.assert_not_called()


def test_log_level_e_aplicado_na_sessao(builder_duble):
    _, sessao = builder_duble

    SparkSessionManager(app_name="teste", master="local[1]", log_level="ERROR").get_or_create()

    sessao.sparkContext.setLogLevel.assert_called_once_with("ERROR")


def test_stop_encerra_a_sessao_e_limpa_a_referencia(builder_duble):
    _, sessao = builder_duble
    manager = SparkSessionManager(app_name="teste", master="local[1]")
    manager.get_or_create()

    manager.stop()

    sessao.stop.assert_called_once()
    assert manager.session is None


def test_stop_sem_sessao_criada_nao_falha():
    manager = SparkSessionManager(app_name="teste", master="local[1]")

    manager.stop()  # nao deve lancar

    assert manager.session is None


@pytest.mark.integration
def test_get_or_create_devolve_uma_sessao_spark_utilizavel(spark):
    """Teste de fumaca: a classe produz uma SparkSession que funciona de fato."""
    manager = SparkSessionManager(app_name="teste-integracao", master="local[1]")

    sessao = manager.get_or_create()

    assert sessao.range(3).count() == 3

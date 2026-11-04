"""Testes unitarios da classe de configuracao (nao dependem do Spark)."""

from __future__ import annotations

import textwrap

import pytest

from pedidos_recusados.config.settings import AppConfig, ConfigError

pytestmark = pytest.mark.unit

YAML_VALIDO = textwrap.dedent(
    """
    spark:
      app_name: "App de Teste"
      master: "local[1]"
      log_level: "ERROR"
      configs:
        spark.sql.shuffle.partitions: "2"
    paths:
      pedidos: "data/input/pedidos"
      pagamentos: "data/input/pagamentos"
      output: "data/output/relatorio"
    file_options:
      pedidos_csv:
        compression: "gzip"
        header: true
        sep: ";"
      pagamentos_json:
        compression: "gzip"
    output_options:
      mode: "overwrite"
      coalesce: 1
    business_rules:
      ano_referencia: 2025
      status_pagamento: false
      fraude: false
    """
)


@pytest.fixture
def arquivo_config(tmp_path):
    caminho = tmp_path / "settings.yaml"
    caminho.write_text(YAML_VALIDO, encoding="utf-8")
    return caminho


def test_load_le_todas_as_secoes(arquivo_config):
    config = AppConfig.load(arquivo_config)

    assert config.app_name == "App de Teste"
    assert config.master == "local[1]"
    assert config.log_level == "ERROR"
    assert config.spark_configs == {"spark.sql.shuffle.partitions": "2"}


def test_load_le_as_regras_de_negocio(arquivo_config):
    config = AppConfig.load(arquivo_config)

    assert config.ano_referencia == 2025
    assert config.status_pagamento is False
    assert config.fraude is False


def test_load_le_as_opcoes_de_arquivo(arquivo_config):
    config = AppConfig.load(arquivo_config)

    assert config.pedidos_options == {"compression": "gzip", "header": True, "sep": ";"}
    assert config.output_options["mode"] == "overwrite"


def test_caminhos_relativos_sao_resolvidos_a_partir_da_raiz(arquivo_config):
    config = AppConfig.load(arquivo_config)

    assert config.pedidos_path.endswith("data/input/pedidos")
    assert config.pedidos_path.startswith("/") or ":" in config.pedidos_path


@pytest.mark.parametrize(
    "valor",
    ["s3://bucket/pedidos", "hdfs://namenode/pedidos", "dbfs:/mnt/pedidos"],
)
def test_resolve_path_preserva_uris(arquivo_config, valor):
    config = AppConfig.load(arquivo_config)

    assert config.resolve_path(valor) == valor


def test_load_falha_quando_o_arquivo_nao_existe(tmp_path):
    with pytest.raises(ConfigError, match="nao encontrado"):
        AppConfig.load(tmp_path / "inexistente.yaml")


def test_load_falha_quando_faltam_secoes(tmp_path):
    caminho = tmp_path / "incompleto.yaml"
    caminho.write_text("spark:\n  app_name: 'X'\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="Secoes ausentes"):
        AppConfig.load(caminho)

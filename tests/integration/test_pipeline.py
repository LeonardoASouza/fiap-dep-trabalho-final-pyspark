"""Testes de integracao da classe de orquestracao do pipeline.

Mostram, na pratica, o ganho da injecao de dependencias: o ``Pipeline``
recebe um ``DataHandler`` dublê (fake), executa a regra de negocio real e
nao toca em disco para ler as fontes.
"""

from __future__ import annotations

import pytest

from pedidos_recusados.config.settings import AppConfig
from pedidos_recusados.io_utils.exceptions import DataReadError
from pedidos_recusados.pipeline.pipeline import Pipeline, PipelineError
from pedidos_recusados.processing.transformations import RelatorioPedidosRecusadosTransformer

pytestmark = pytest.mark.integration


class FakeDataHandler:
    """Dublê do ``DataHandler``: devolve dataframes em memoria e registra a escrita."""

    def __init__(self, pedidos_df, pagamentos_df, erro_na_leitura: bool = False):
        self._pedidos_df = pedidos_df
        self._pagamentos_df = pagamentos_df
        self._erro_na_leitura = erro_na_leitura
        self.escrito = None
        self.path_escrito = None

    def read_pedidos(self, path, options=None):
        if self._erro_na_leitura:
            raise DataReadError(f"fonte indisponivel: {path}")
        return self._pedidos_df

    def read_pagamentos(self, path, options=None):
        return self._pagamentos_df

    def write_parquet(self, df, path, options=None):
        self.escrito = df.collect()
        self.path_escrito = path


@pytest.fixture
def config(tmp_path):
    arquivo = tmp_path / "settings.yaml"
    arquivo.write_text(
        "spark:\n"
        "  app_name: 'Pipeline de Teste'\n"
        "paths:\n"
        "  pedidos: 'data/input/pedidos'\n"
        "  pagamentos: 'data/input/pagamentos'\n"
        f"  output: '{tmp_path / 'saida'}'\n"
        "file_options:\n"
        "  pedidos_csv: {compression: 'gzip', header: true, sep: ';'}\n"
        "  pagamentos_json: {compression: 'gzip'}\n"
        "output_options:\n"
        "  mode: 'overwrite'\n"
        "  coalesce: 1\n"
        "business_rules:\n"
        "  ano_referencia: 2025\n"
        "  status_pagamento: false\n"
        "  fraude: false\n",
        encoding="utf-8",
    )
    return AppConfig.load(arquivo)


def test_pipeline_executa_fim_a_fim_com_dependencias_injetadas(
    config, schemas, pedidos_df, pagamentos_df
):
    data_handler = FakeDataHandler(pedidos_df, pagamentos_df)
    transformer = RelatorioPedidosRecusadosTransformer(
        schemas=schemas, ano_referencia=config.ano_referencia
    )
    pipeline = Pipeline(config=config, data_handler=data_handler, transformer=transformer)

    relatorio = pipeline.run()

    assert relatorio.count() == 2
    assert [linha.id_pedido for linha in data_handler.escrito] == ["p2", "p1"]
    assert data_handler.path_escrito == config.output_path


def test_pipeline_converte_erro_de_io_em_pipeline_error(config, schemas, pedidos_df, pagamentos_df):
    data_handler = FakeDataHandler(pedidos_df, pagamentos_df, erro_na_leitura=True)
    transformer = RelatorioPedidosRecusadosTransformer(schemas=schemas)
    pipeline = Pipeline(config=config, data_handler=data_handler, transformer=transformer)

    with pytest.raises(PipelineError, match="fonte indisponivel"):
        pipeline.run()

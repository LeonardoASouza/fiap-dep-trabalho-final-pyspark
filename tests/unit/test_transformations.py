"""Testes unitarios da classe de logica de negocio.

A classe ``RelatorioPedidosRecusadosTransformer`` concentra as regras do
escopo de negocio, portanto e a mais critica do projeto.
"""

from __future__ import annotations

import datetime

import pytest

from pedidos_recusados.processing.transformations import (
    BusinessLogicError,
    RelatorioPedidosRecusadosTransformer,
)

pytestmark = pytest.mark.unit


@pytest.fixture
def transformer(schemas) -> RelatorioPedidosRecusadosTransformer:
    return RelatorioPedidosRecusadosTransformer(schemas=schemas, ano_referencia=2025)


def test_filtrar_pedidos_do_ano_mantem_apenas_2025(transformer, pedidos_df):
    # Arrange / Act
    resultado = transformer.filtrar_pedidos_do_ano(pedidos_df)

    # Assert
    ids = sorted(linha.id_pedido for linha in resultado.collect())
    assert ids == ["p1", "p2", "p3"]


def test_calcular_valor_total_multiplica_valor_unitario_por_quantidade(transformer, pedidos_df):
    resultado = transformer.calcular_valor_total(pedidos_df)

    valores = {linha.id_pedido: linha.valor_total_pedido for linha in resultado.collect()}
    assert valores["p1"] == 3000.0  # 1500,00 x 2
    assert valores["p3"] == 1800.0  # 600,00 x 3


def test_filtrar_pagamentos_mantem_recusados_e_legitimos(transformer, pagamentos_df):
    resultado = transformer.filtrar_pagamentos_recusados_legitimos(pagamentos_df)

    ids = sorted(linha.id_pedido for linha in resultado.collect())
    assert ids == ["p1", "p2"]


def test_transformar_produz_relatorio_completo_e_ordenado(transformer, pedidos_df, pagamentos_df):
    relatorio = transformer.transformar(pedidos_df, pagamentos_df)
    linhas = relatorio.collect()

    # Apenas os pedidos de 2025 com pagamento recusado e fraude=false.
    assert [linha.id_pedido for linha in linhas] == ["p2", "p1"]

    # Ordenacao exigida: UF, forma de pagamento, data do pedido.
    assert [(linha.uf, linha.forma_pagamento) for linha in linhas] == [
        ("SP", "BOLETO"),
        ("SP", "PIX"),
    ]

    primeira = linhas[0]
    assert primeira.valor_total_pedido == 1000.0
    assert primeira.data_pedido == datetime.date(2025, 1, 5)


def test_transformar_respeita_o_schema_explicito_do_relatorio(
    transformer, schemas, pedidos_df, pagamentos_df
):
    relatorio = transformer.transformar(pedidos_df, pagamentos_df)

    assert relatorio.columns == schemas.relatorio_columns()
    assert [f.dataType.simpleString() for f in relatorio.schema.fields] == [
        f.dataType.simpleString() for f in schemas.relatorio().fields
    ]


@pytest.mark.parametrize(
    "ano, ids_esperados",
    [
        (2025, ["p1", "p2", "p3"]),
        (2024, ["p4"]),
        (2023, []),
    ],
)
def test_filtrar_pedidos_do_ano_parametrizado(schemas, pedidos_df, ano, ids_esperados):
    transformer = RelatorioPedidosRecusadosTransformer(schemas=schemas, ano_referencia=ano)

    resultado = transformer.filtrar_pedidos_do_ano(pedidos_df)

    assert sorted(linha.id_pedido for linha in resultado.collect()) == ids_esperados


def test_transformar_sem_correspondencia_retorna_relatorio_vazio(
    transformer, spark, schemas, pedidos_df
):
    pagamentos_vazio = spark.createDataFrame([], schema=schemas.pagamentos())

    relatorio = transformer.transformar(pedidos_df, pagamentos_vazio)

    assert relatorio.count() == 0
    assert relatorio.columns == schemas.relatorio_columns()


def test_transformar_lanca_erro_de_negocio_quando_a_coluna_nao_existe(
    transformer, pedidos_df, pagamentos_df
):
    pedidos_sem_data = pedidos_df.drop("data_criacao")

    with pytest.raises(BusinessLogicError):
        transformer.transformar(pedidos_sem_data, pagamentos_df)


def test_validar_schema_rejeita_relatorio_divergente(transformer, pedidos_df, pagamentos_df):
    relatorio = transformer.transformar(pedidos_df, pagamentos_df)
    divergente = relatorio.drop("uf")

    with pytest.raises(BusinessLogicError, match="Schema do relatorio divergente"):
        transformer.validar_schema(divergente)

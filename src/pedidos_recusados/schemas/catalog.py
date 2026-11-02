"""Catalogo de schemas EXPLICITOS de todos os dataframes do projeto.

Nenhuma leitura do projeto usa ``inferSchema``. Centralizar os schemas em
uma unica classe garante que:

* os tipos sao estaveis entre execucoes (nao dependem da amostra lida);
* a leitura e mais rapida (o Spark nao precisa varrer os dados duas vezes);
* o contrato de dados fica explicito e versionado junto com o codigo.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class SchemaCatalog:
    """Fornece os schemas explicitos usados na leitura e na escrita."""

    # ------------------------------------------------------------------
    # Fontes
    # ------------------------------------------------------------------
    @staticmethod
    def pedidos() -> StructType:
        """Schema do dataset de pedidos (CSV, separador ';', compactado em gzip).

        Leiaute oficial: ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;
        DATA_CRIACAO;UF;ID_CLIENTE
        """
        return StructType(
            [
                StructField("id_pedido", StringType(), nullable=False),
                StructField("produto", StringType(), nullable=True),
                StructField("valor_unitario", DoubleType(), nullable=True),
                StructField("quantidade", LongType(), nullable=True),
                StructField("data_criacao", TimestampType(), nullable=True),
                StructField("uf", StringType(), nullable=True),
                StructField("id_cliente", LongType(), nullable=True),
            ]
        )

    @staticmethod
    def avaliacao_fraude() -> StructType:
        """Schema do objeto aninhado ``avaliacao_fraude`` do dataset de pagamentos."""
        return StructType(
            [
                StructField("fraude", BooleanType(), nullable=True),
                StructField("score", DoubleType(), nullable=True),
            ]
        )

    @classmethod
    def pagamentos(cls) -> StructType:
        """Schema do dataset de pagamentos (JSON lines, compactado em gzip)."""
        return StructType(
            [
                StructField("id_pedido", StringType(), nullable=False),
                StructField("forma_pagamento", StringType(), nullable=True),
                StructField("valor_pagamento", DoubleType(), nullable=True),
                StructField("status", BooleanType(), nullable=True),
                StructField("data_processamento", TimestampType(), nullable=True),
                StructField("avaliacao_fraude", cls.avaliacao_fraude(), nullable=True),
            ]
        )

    # ------------------------------------------------------------------
    # Saida
    # ------------------------------------------------------------------
    @staticmethod
    def relatorio() -> StructType:
        """Schema do relatorio final solicitado pela alta gestao."""
        return StructType(
            [
                StructField("id_pedido", StringType(), nullable=False),
                StructField("uf", StringType(), nullable=True),
                StructField("forma_pagamento", StringType(), nullable=True),
                StructField("valor_total_pedido", DoubleType(), nullable=True),
                StructField("data_pedido", DateType(), nullable=True),
            ]
        )

    @classmethod
    def relatorio_columns(cls) -> list[str]:
        """Ordem canonica das colunas do relatorio."""
        return [field.name for field in cls.relatorio().fields]

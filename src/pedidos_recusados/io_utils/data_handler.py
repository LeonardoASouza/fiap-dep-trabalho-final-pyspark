"""Classe de leitura e escrita de dados (I/O).

Toda interacao com o sistema de arquivos passa por aqui. As regras de
negocio (pacote ``processing``) nunca leem ou escrevem dados diretamente.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pedidos_recusados.io_utils.exceptions import DataReadError, DataWriteError
from pedidos_recusados.schemas.catalog import SchemaCatalog

logger = logging.getLogger(__name__)


class DataHandler:
    """Le as fontes do projeto e grava o resultado em parquet.

    Recebe por injecao a ``SparkSession`` e o ``SchemaCatalog``: nao cria
    nenhuma das duas dependencias, o que permite substitui-las por dublês
    nos testes.
    """

    def __init__(self, spark: SparkSession, schemas: SchemaCatalog):
        self.spark = spark
        self.schemas = schemas

    # ------------------------------------------------------------------
    # Leitura
    # ------------------------------------------------------------------
    def read_pedidos(self, path: str, options: dict[str, Any] | None = None) -> DataFrame:
        """Le o dataset de pedidos (CSV compactado) com schema explicito."""
        options = options or {}
        schema: StructType = self.schemas.pedidos()
        logger.info("Lendo o dataset de pedidos em: %s", path)

        try:
            df = (
                self.spark.read.option("compression", options.get("compression", "gzip"))
                .option("header", str(options.get("header", True)).lower())
                .option("sep", options.get("sep", ";"))
                .option("mode", "PERMISSIVE")
                .schema(schema)
                .csv(path)
            )
        except AnalysisException as exc:
            raise DataReadError(f"Nao foi possivel ler os pedidos em '{path}': {exc}") from exc
        except PySparkException as exc:  # pragma: no cover - falha de ambiente
            raise DataReadError(f"Falha do Spark ao ler os pedidos em '{path}': {exc}") from exc

        logger.info("Dataset de pedidos aberto com %d colunas", len(df.columns))
        return df

    def read_pagamentos(self, path: str, options: dict[str, Any] | None = None) -> DataFrame:
        """Le o dataset de pagamentos (JSON lines compactado) com schema explicito."""
        options = options or {}
        schema: StructType = self.schemas.pagamentos()
        logger.info("Lendo o dataset de pagamentos em: %s", path)

        try:
            reader = self.spark.read.option(
                "compression", options.get("compression", "gzip")
            ).option("mode", "PERMISSIVE")

            timestamp_format = options.get("timestamp_format")
            if timestamp_format:
                reader = reader.option("timestampFormat", timestamp_format)

            df = reader.schema(schema).json(path)
        except AnalysisException as exc:
            raise DataReadError(f"Nao foi possivel ler os pagamentos em '{path}': {exc}") from exc
        except PySparkException as exc:  # pragma: no cover - falha de ambiente
            raise DataReadError(f"Falha do Spark ao ler os pagamentos em '{path}': {exc}") from exc

        logger.info("Dataset de pagamentos aberto com %d colunas", len(df.columns))
        return df

    # ------------------------------------------------------------------
    # Escrita
    # ------------------------------------------------------------------
    def write_parquet(
        self, df: DataFrame, path: str, options: dict[str, Any] | None = None
    ) -> None:
        """Grava um dataframe em formato parquet."""
        options = options or {}
        mode = options.get("mode", "overwrite")
        compression = options.get("compression", "snappy")
        coalesce = options.get("coalesce")

        logger.info("Gravando o resultado em parquet: %s (mode=%s)", path, mode)

        try:
            target = df.coalesce(int(coalesce)) if coalesce else df
            target.write.mode(mode).option("compression", compression).parquet(path)
        except AnalysisException as exc:
            raise DataWriteError(f"Nao foi possivel gravar em '{path}': {exc}") from exc
        except PySparkException as exc:  # pragma: no cover - falha de ambiente
            raise DataWriteError(f"Falha do Spark ao gravar em '{path}': {exc}") from exc

        logger.info("Resultado gravado com sucesso em: %s", path)

    def read_parquet(self, path: str, schema: StructType | None = None) -> DataFrame:
        """Le um parquet ja gravado (usado para conferencia do resultado)."""
        logger.info("Lendo o parquet de conferencia em: %s", path)
        try:
            reader = self.spark.read
            if schema is not None:
                reader = reader.schema(schema)
            return reader.parquet(path)
        except AnalysisException as exc:
            raise DataReadError(f"Nao foi possivel ler o parquet em '{path}': {exc}") from exc

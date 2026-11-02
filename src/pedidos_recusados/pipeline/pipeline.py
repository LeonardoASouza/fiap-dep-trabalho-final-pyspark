"""Classe de orquestracao do pipeline.

A classe ``Pipeline`` NAO cria suas dependencias: ela as recebe prontas no
construtor (injecao de dependencias). Quem monta e injeta tudo e a raiz de
composicao, o ``main.py``.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from pedidos_recusados.config.settings import AppConfig
from pedidos_recusados.io_utils.data_handler import DataHandler
from pedidos_recusados.io_utils.exceptions import IOLayerError
from pedidos_recusados.processing.transformations import (
    BusinessLogicError,
    RelatorioPedidosRecusadosTransformer,
)

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Falha na execucao do pipeline."""


class Pipeline:
    """Orquestra as etapas de leitura, transformacao e escrita."""

    def __init__(
        self,
        config: AppConfig,
        data_handler: DataHandler,
        transformer: RelatorioPedidosRecusadosTransformer,
        preview_rows: int = 10,
    ):
        self.config = config
        self.data_handler = data_handler
        self.transformer = transformer
        self.preview_rows = preview_rows

    def run(self) -> DataFrame:
        """Executa o pipeline fim a fim e devolve o relatorio gerado."""
        logger.info("===== Pipeline iniciado: %s =====", self.config.app_name)

        try:
            # 1. Extracao
            pedidos_df = self.data_handler.read_pedidos(
                path=self.config.pedidos_path, options=self.config.pedidos_options
            )
            pagamentos_df = self.data_handler.read_pagamentos(
                path=self.config.pagamentos_path, options=self.config.pagamentos_options
            )

            # 2. Transformacao (regras de negocio)
            relatorio_df = self.transformer.transformar(pedidos_df, pagamentos_df)
            relatorio_df = relatorio_df.cache()

            total = relatorio_df.count()
            logger.info("Relatorio gerado com %d pedidos", total)
            relatorio_df.show(self.preview_rows, truncate=False)

            # 3. Carga
            self.data_handler.write_parquet(
                df=relatorio_df,
                path=self.config.output_path,
                options=self.config.output_options,
            )

            logger.info("===== Pipeline concluido com sucesso =====")
            return relatorio_df

        except (IOLayerError, BusinessLogicError) as exc:
            logger.error("Pipeline interrompido: %s", exc)
            raise PipelineError(str(exc)) from exc
        except Exception as exc:  # pragma: no cover - rede de seguranca
            logger.exception("Erro inesperado na execucao do pipeline: %s", exc)
            raise PipelineError("Falha inesperada na execucao do pipeline") from exc

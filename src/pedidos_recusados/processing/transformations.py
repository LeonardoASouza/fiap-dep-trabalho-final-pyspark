"""Logica de negocio do relatorio solicitado pela alta gestao.

Regra de negocio
----------------
Relatorio de pedidos de venda cujo pagamento foi RECUSADO (``status = false``)
e que, na avaliacao de fraude, foram classificados como LEGITIMOS
(``avaliacao_fraude.fraude = false``), restrito ao ano de 2025 e ordenado por
UF, forma de pagamento e data de criacao do pedido.

Esta camada e pura: recebe dataframes prontos e devolve dataframes
transformados. Ela nao le nem grava arquivos (isso e papel do ``DataHandler``)
e nao cria sessao Spark (papel do ``SparkSessionManager``).
"""

from __future__ import annotations

import logging

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from pedidos_recusados.schemas.catalog import SchemaCatalog

# Configuracao do logging da aplicacao (criterio 9 do trabalho).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


class BusinessLogicError(Exception):
    """Falha na aplicacao das regras de negocio do relatorio."""


class RelatorioPedidosRecusadosTransformer:
    """Aplica as regras de negocio que produzem o relatorio final."""

    def __init__(
        self,
        schemas: SchemaCatalog,
        ano_referencia: int = 2025,
        status_pagamento: bool = False,
        fraude: bool = False,
    ):
        self.schemas = schemas
        self.ano_referencia = ano_referencia
        self.status_pagamento = status_pagamento
        self.fraude = fraude

    # ------------------------------------------------------------------
    # Etapas
    # ------------------------------------------------------------------
    def filtrar_pedidos_do_ano(self, pedidos_df: DataFrame) -> DataFrame:
        """Mantem apenas os pedidos criados no ano de referencia (2025)."""
        try:
            logger.info("Filtrando pedidos do ano de referencia: %d", self.ano_referencia)
            return pedidos_df.filter(F.year(F.col("data_criacao")) == F.lit(self.ano_referencia))
        except (AnalysisException, PySparkException) as exc:
            logger.error("Erro ao filtrar os pedidos por ano: %s", exc)
            raise BusinessLogicError("Falha ao filtrar os pedidos pelo ano de referencia") from exc

    def calcular_valor_total(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna ``valor_total_pedido`` = valor_unitario * quantidade."""
        try:
            logger.info("Calculando o valor total de cada pedido")
            return pedidos_df.withColumn(
                "valor_total_pedido",
                F.round(F.col("valor_unitario") * F.col("quantidade"), 2),
            )
        except (AnalysisException, PySparkException) as exc:
            logger.error("Erro ao calcular o valor total do pedido: %s", exc)
            raise BusinessLogicError("Falha ao calcular o valor total do pedido") from exc

    def filtrar_pagamentos_recusados_legitimos(self, pagamentos_df: DataFrame) -> DataFrame:
        """Mantem pagamentos recusados (status=false) e legitimos (fraude=false)."""
        try:
            logger.info(
                "Filtrando pagamentos com status=%s e avaliacao_fraude.fraude=%s",
                self.status_pagamento,
                self.fraude,
            )
            return pagamentos_df.filter(
                (F.col("status") == F.lit(self.status_pagamento))
                & (F.col("avaliacao_fraude.fraude") == F.lit(self.fraude))
            )
        except (AnalysisException, PySparkException) as exc:
            logger.error("Erro ao filtrar os pagamentos recusados e legitimos: %s", exc)
            raise BusinessLogicError(
                "Falha ao filtrar os pagamentos recusados e legitimos"
            ) from exc

    def montar_relatorio(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """Junta pedidos e pagamentos, seleciona e ordena os atributos do relatorio."""
        try:
            logger.info("Juntando pedidos e pagamentos pelo id_pedido")
            relatorio_df = (
                pedidos_df.alias("ped")
                .join(pagamentos_df.alias("pag"), on="id_pedido", how="inner")
                .select(
                    F.col("id_pedido").cast("string").alias("id_pedido"),
                    F.col("ped.uf").cast("string").alias("uf"),
                    F.col("pag.forma_pagamento").cast("string").alias("forma_pagamento"),
                    F.col("ped.valor_total_pedido").cast("double").alias("valor_total_pedido"),
                    F.to_date(F.col("ped.data_criacao")).alias("data_pedido"),
                )
            )

            logger.info("Ordenando o relatorio por uf, forma_pagamento e data_pedido")
            relatorio_df = relatorio_df.orderBy(
                F.col("uf").asc(),
                F.col("forma_pagamento").asc(),
                F.col("data_pedido").asc(),
                # Desempate deterministico: garante o mesmo resultado a cada execucao.
                F.col("id_pedido").asc(),
            )
            return relatorio_df
        except (AnalysisException, PySparkException) as exc:
            logger.error("Erro ao montar o relatorio: %s", exc)
            raise BusinessLogicError("Falha ao montar o relatorio final") from exc

    # ------------------------------------------------------------------
    # Fluxo completo da regra de negocio
    # ------------------------------------------------------------------
    def transformar(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """Executa, em ordem, todas as etapas da regra de negocio."""
        logger.info("Iniciando as transformacoes de negocio")
        try:
            pedidos_ano_df = self.filtrar_pedidos_do_ano(pedidos_df)
            pedidos_valorados_df = self.calcular_valor_total(pedidos_ano_df)
            pagamentos_filtrados_df = self.filtrar_pagamentos_recusados_legitimos(pagamentos_df)
            relatorio_df = self.montar_relatorio(pedidos_valorados_df, pagamentos_filtrados_df)
            self.validar_schema(relatorio_df)
            logger.info("Transformacoes de negocio concluidas")
            return relatorio_df
        except BusinessLogicError:
            raise
        except Exception as exc:  # pragma: no cover - rede de seguranca
            logger.error("Erro inesperado nas transformacoes de negocio: %s", exc)
            raise BusinessLogicError("Falha inesperada nas transformacoes de negocio") from exc

    # ------------------------------------------------------------------
    # Qualidade
    # ------------------------------------------------------------------
    def validar_schema(self, relatorio_df: DataFrame) -> None:
        """Confere se o relatorio produzido respeita o schema explicito esperado."""
        esperado: StructType = self.schemas.relatorio()
        obtido = [(f.name, f.dataType.simpleString()) for f in relatorio_df.schema.fields]
        alvo = [(f.name, f.dataType.simpleString()) for f in esperado.fields]

        if obtido != alvo:
            logger.error("Schema do relatorio divergente. Esperado=%s | Obtido=%s", alvo, obtido)
            raise BusinessLogicError(
                f"Schema do relatorio divergente. Esperado={alvo} | Obtido={obtido}"
            )

        logger.info("Schema do relatorio validado: %s", alvo)

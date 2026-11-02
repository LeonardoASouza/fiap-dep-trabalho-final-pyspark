"""Classe responsavel pelo ciclo de vida da sessao Spark."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Cria, expoe e encerra a ``SparkSession`` da aplicacao.

    A classe e instanciada na raiz de composicao (``main.py``) com os
    parametros vindos da classe de configuracao e injetada onde for
    necessaria. Nenhum outro componente cria sessoes Spark.
    """

    def __init__(
        self,
        app_name: str,
        master: str | None = None,
        configs: dict[str, str] | None = None,
        log_level: str = "WARN",
    ):
        self.app_name = app_name
        self.master = master
        self.configs = configs or {}
        self.log_level = log_level
        self._session: SparkSession | None = None

    def get_or_create(self) -> SparkSession:
        """Devolve a sessao Spark, criando-a na primeira chamada."""
        if self._session is not None:
            return self._session

        logger.info("Criando a SparkSession '%s'", self.app_name)
        builder = SparkSession.builder.appName(self.app_name)

        # Quando o master nao e informado, respeita o que vier do
        # spark-submit (yarn, k8s, standalone...) - projeto agnostico.
        if self.master:
            builder = builder.master(self.master)

        for key, value in self.configs.items():
            builder = builder.config(key, str(value))

        self._session = builder.getOrCreate()
        self._session.sparkContext.setLogLevel(self.log_level)

        logger.info(
            "SparkSession ativa | versao=%s | master=%s",
            self._session.version,
            self._session.sparkContext.master,
        )
        return self._session

    @property
    def session(self) -> SparkSession | None:
        """Sessao ja criada (ou ``None``), sem efeito colateral."""
        return self._session

    def stop(self) -> None:
        """Encerra a sessao Spark, se houver."""
        if self._session is not None:
            logger.info("Encerrando a SparkSession '%s'", self.app_name)
            self._session.stop()
            self._session = None

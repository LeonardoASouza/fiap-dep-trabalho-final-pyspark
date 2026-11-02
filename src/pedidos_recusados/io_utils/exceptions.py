"""Excecoes customizadas da camada de I/O.

Traduzem erros tecnicos do Spark (``AnalysisException``, ``PySparkException``)
para erros de dominio da aplicacao, de modo que as camadas superiores nao
precisem conhecer detalhes de implementacao do Spark.
"""


class IOLayerError(Exception):
    """Erro generico da camada de leitura/escrita de dados."""


class DataReadError(IOLayerError):
    """Falha na leitura de uma fonte de dados."""


class DataWriteError(IOLayerError):
    """Falha na escrita do resultado."""

"""Raiz de composicao (Aggregation Root) da aplicacao.

Este e o UNICO lugar do projeto que instancia dependencias concretas.
Todos os demais componentes recebem suas dependencias ja prontas, via
construtor (Injecao de Dependencias).

Execucao:
    spark-submit src/pedidos_recusados/main.py
    python -m pedidos_recusados.main
    run-pedidos-recusados          # entry point instalado pelo pacote
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Permite executar o arquivo diretamente (`spark-submit src/pedidos_recusados/main.py`)
# mesmo quando o pacote ainda nao foi instalado com `pip install -e .`:
# garante que o diretorio `src/` esteja no sys.path antes dos imports do projeto.
_SRC_DIR = str(Path(__file__).resolve().parents[1])
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from pedidos_recusados.config.settings import AppConfig, ConfigError  # noqa: E402
from pedidos_recusados.io_utils.data_handler import DataHandler  # noqa: E402
from pedidos_recusados.pipeline.pipeline import Pipeline, PipelineError  # noqa: E402
from pedidos_recusados.processing.transformations import (  # noqa: E402
    RelatorioPedidosRecusadosTransformer,
)
from pedidos_recusados.schemas.catalog import SchemaCatalog  # noqa: E402
from pedidos_recusados.session.spark_session import SparkSessionManager  # noqa: E402

logger = logging.getLogger(__name__)


def main(config_path: str | None = None) -> int:
    """Monta o grafo de dependencias e dispara o pipeline."""
    session_manager: SparkSessionManager | None = None

    try:
        # --- 1. Configuracao centralizada -----------------------------
        config = AppConfig.load(config_path)
        logger.info("Configuracao carregada: %s", config)

        # --- 2. Sessao Spark ------------------------------------------
        session_manager = SparkSessionManager(
            app_name=config.app_name,
            master=config.master,
            configs=config.spark_configs,
            log_level=config.log_level,
        )
        spark = session_manager.get_or_create()

        # --- 3. Raiz de composicao: instanciar e injetar --------------
        schemas = SchemaCatalog()
        data_handler = DataHandler(spark=spark, schemas=schemas)
        transformer = RelatorioPedidosRecusadosTransformer(
            schemas=schemas,
            ano_referencia=config.ano_referencia,
            status_pagamento=config.status_pagamento,
            fraude=config.fraude,
        )
        pipeline = Pipeline(
            config=config,
            data_handler=data_handler,
            transformer=transformer,
        )

        # --- 4. Execucao ----------------------------------------------
        pipeline.run()
        return 0

    except (ConfigError, PipelineError) as exc:
        logger.error("Execucao encerrada com erro: %s", exc)
        return 1
    finally:
        if session_manager is not None:
            session_manager.stop()


if __name__ == "__main__":
    sys.exit(main(config_path=sys.argv[1] if len(sys.argv) > 1 else None))

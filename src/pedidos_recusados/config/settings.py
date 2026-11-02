"""Classe de configuracao centralizada da aplicacao.

Toda a parametrizacao do pipeline (caminhos, opcoes de leitura/escrita,
regras de negocio e propriedades do Spark) vive no arquivo YAML
``config/settings.yaml`` e e exposta ao restante da aplicacao por esta classe.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Erro na leitura ou validacao do arquivo de configuracao."""


class AppConfig:
    """Encapsula as configuracoes da aplicacao lidas de um arquivo YAML.

    A classe e instanciada uma unica vez na raiz de composicao (``main.py``)
    e injetada nos demais componentes, de modo que nenhum deles precise
    conhecer a origem (arquivo, variavel de ambiente, etc.) dos parametros.
    """

    #: Variavel de ambiente que permite sobrescrever o arquivo de configuracao.
    ENV_VAR = "PEDIDOS_RECUSADOS_CONFIG"

    #: Caminho padrao, relativo a raiz do projeto.
    DEFAULT_RELATIVE_PATH = Path("config") / "settings.yaml"

    #: Identifica URIs de sistemas de arquivos distribuidos (s3://, hdfs://, dbfs:/...).
    URI_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]+:")

    #: Chaves obrigatorias no arquivo de configuracao.
    REQUIRED_SECTIONS = (
        "spark",
        "paths",
        "file_options",
        "output_options",
        "business_rules",
    )

    def __init__(self, data: dict[str, Any], project_root: Path | str | None = None):
        self._data = data
        self._project_root = Path(project_root) if project_root else self.project_root_path()
        self._validate()

    # ------------------------------------------------------------------
    # Construcao
    # ------------------------------------------------------------------
    @classmethod
    def project_root_path(cls) -> Path:
        """Raiz do projeto, deduzida a partir da localizacao deste modulo.

        ``src/pedidos_recusados/config/settings.py`` -> sobe 4 niveis.
        Mantem o projeto agnostico ao diretorio de onde ele e executado.
        """
        return Path(__file__).resolve().parents[3]

    @classmethod
    def load(cls, path: str | Path | None = None) -> AppConfig:
        """Carrega as configuracoes de um arquivo YAML.

        Ordem de precedencia do caminho:
        1. argumento ``path``;
        2. variavel de ambiente ``PEDIDOS_RECUSADOS_CONFIG``;
        3. ``<raiz do projeto>/config/settings.yaml``.
        """
        root = cls.project_root_path()
        candidate = path or os.environ.get(cls.ENV_VAR) or (root / cls.DEFAULT_RELATIVE_PATH)
        config_path = Path(candidate)

        if not config_path.is_absolute():
            config_path = (root / config_path).resolve()

        logger.info("Carregando configuracoes de: %s", config_path)

        try:
            with open(config_path, encoding="utf-8") as handler:
                data = yaml.safe_load(handler)
        except FileNotFoundError as exc:
            raise ConfigError(f"Arquivo de configuracao nao encontrado: {config_path}") from exc
        except yaml.YAMLError as exc:
            raise ConfigError(f"Arquivo de configuracao invalido: {config_path}") from exc

        if not isinstance(data, dict):
            raise ConfigError(f"Conteudo do arquivo de configuracao nao e um mapa: {config_path}")

        return cls(data=data, project_root=root)

    def _validate(self) -> None:
        faltantes = [s for s in self.REQUIRED_SECTIONS if s not in self._data]
        if faltantes:
            raise ConfigError(f"Secoes ausentes no arquivo de configuracao: {', '.join(faltantes)}")

    # ------------------------------------------------------------------
    # Acesso generico
    # ------------------------------------------------------------------
    @property
    def raw(self) -> dict[str, Any]:
        """Dicionario bruto, util para inspecao e testes."""
        return self._data

    def section(self, name: str) -> dict[str, Any]:
        try:
            return self._data[name]
        except KeyError as exc:
            raise ConfigError(f"Secao inexistente na configuracao: {name}") from exc

    def resolve_path(self, value: str) -> str:
        """Resolve um caminho da configuracao.

        Caminhos relativos sao resolvidos a partir da raiz do projeto;
        caminhos absolutos e URIs (``s3://``, ``hdfs://``, ``dbfs:/``...)
        sao devolvidos sem alteracao - e o que mantem o projeto agnostico
        a plataforma de execucao.

        O padrao exige pelo menos dois caracteres antes dos dois-pontos,
        para nao confundir uma URI com uma letra de unidade do Windows
        (``C:\\dados``).
        """
        if self.URI_PATTERN.match(value) or Path(value).is_absolute():
            return value
        return str((self._project_root / value).resolve())

    # ------------------------------------------------------------------
    # Spark
    # ------------------------------------------------------------------
    @property
    def app_name(self) -> str:
        return self.section("spark")["app_name"]

    @property
    def master(self) -> str | None:
        return self.section("spark").get("master") or None

    @property
    def log_level(self) -> str:
        return self.section("spark").get("log_level", "WARN")

    @property
    def spark_configs(self) -> dict[str, str]:
        return dict(self.section("spark").get("configs") or {})

    # ------------------------------------------------------------------
    # Caminhos
    # ------------------------------------------------------------------
    @property
    def pedidos_path(self) -> str:
        return self.resolve_path(self.section("paths")["pedidos"])

    @property
    def pagamentos_path(self) -> str:
        return self.resolve_path(self.section("paths")["pagamentos"])

    @property
    def output_path(self) -> str:
        return self.resolve_path(self.section("paths")["output"])

    # ------------------------------------------------------------------
    # Opcoes de arquivo
    # ------------------------------------------------------------------
    @property
    def pedidos_options(self) -> dict[str, Any]:
        return dict(self.section("file_options")["pedidos_csv"])

    @property
    def pagamentos_options(self) -> dict[str, Any]:
        return dict(self.section("file_options")["pagamentos_json"])

    @property
    def output_options(self) -> dict[str, Any]:
        return dict(self.section("output_options"))

    # ------------------------------------------------------------------
    # Regras de negocio
    # ------------------------------------------------------------------
    @property
    def ano_referencia(self) -> int:
        return int(self.section("business_rules")["ano_referencia"])

    @property
    def status_pagamento(self) -> bool:
        return bool(self.section("business_rules")["status_pagamento"])

    @property
    def fraude(self) -> bool:
        return bool(self.section("business_rules")["fraude"])

    def __repr__(self) -> str:  # pragma: no cover - representacao auxiliar
        return (
            f"AppConfig(app_name={self.app_name!r}, ano_referencia={self.ano_referencia}, "
            f"output_path={self.output_path!r})"
        )

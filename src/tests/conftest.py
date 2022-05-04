import logging
import os
import random
import shutil
import sys
from pathlib import Path

import pytest

try:
    from src.configs import PASTA_ENTRADA_AQUISICAO
except ModuleNotFoundError:
    sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent))


@pytest.fixture(scope="session")
def test_path() -> Path:
    caminho = Path(os.path.dirname(__file__)) / "data"
    caminho.mkdir(parents=True, exist_ok=True)

    yield caminho

    logging.shutdown()
    shutil.rmtree(caminho)


@pytest.fixture(scope="session")
def dados_path() -> Path:
    return Path(os.path.dirname(__file__)) / "dados"


@pytest.fixture(scope="session")
def ano(dados_path: Path) -> int:
    anos = [int(f[:4]) for f in os.listdir(dados_path / "externo/censo_escolar")]
    return random.choice(anos)

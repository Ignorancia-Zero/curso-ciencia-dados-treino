import logging
import shutil
import sys

import pytest
from pathlib import Path

try:
    from src.configs import PASTA_ENTRADA_AQUISICAO
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).parent.parent.parent))


@pytest.fixture(scope="session")
def test_path():
    caminho = Path(__file__).parent / "data"
    caminho.mkdir(parents=True, exist_ok=True)

    yield caminho

    logging.shutdown()
    shutil.rmtree(caminho)

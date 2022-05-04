import logging
import os
import shutil
import sys
from pathlib import Path

import pytest

try:
    from src.configs import PASTA_ENTRADA_AQUISICAO
except ModuleNotFoundError:
    sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent))


@pytest.fixture(scope="session")
def test_path():
    caminho = Path(os.path.dirname(__file__)) / "data"
    caminho.mkdir(parents=True, exist_ok=True)

    yield caminho

    logging.shutdown()
    shutil.rmtree(caminho)

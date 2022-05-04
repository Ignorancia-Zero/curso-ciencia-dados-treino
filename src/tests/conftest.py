import os
import sys
from pathlib import Path

import pytest

try:
    from src.configs import PASTA_ENTRADA_AQUISICAO
except ModuleNotFoundError:
    sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent))


@pytest.fixture(scope="session")
def test_path():
    return Path(os.path.dirname(__file__))

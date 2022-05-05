import logging
import os
import unittest
from pathlib import Path

import pytest

from src.utils.logs import configura_logs
from src.utils.logs import log_erros


@pytest.fixture(scope="module")
def chave(test_path: Path) -> str:
    return configura_logs(pasta_logs=test_path / "logs")


@pytest.mark.order(2)
def test_configura_logs(chave: str, test_path: Path):
    assert isinstance(chave, str)
    assert os.path.exists(test_path / "logs")
    assert os.path.exists(test_path / f"logs/{chave}.log")

    logger = logging.getLogger(__name__)
    logger.info("Olá mundo!")

    with open(test_path / f"logs/{chave}.log") as f:
        texto = f.read()
        assert "Olá mundo!" in texto


@pytest.mark.order(1)
def test_log_erros(chave: str, test_path: Path):
    @log_erros
    def jesus():
        raise ValueError("Não deu certo amigão")

    try:
        jesus()
    except:
        pass

    with open(test_path / f"logs/{chave}.log") as f:
        texto = f.read()
        assert "ValueError: Não deu certo amigão" in texto


if __name__ == "__main__":
    unittest.main()

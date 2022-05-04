import logging
import os
from pathlib import Path

import pytest

from src.utils.logs import configura_logs
from src.utils.logs import log_erros


@pytest.fixture(scope="module")
def chave(test_path: Path) -> str:
    return configura_logs(pasta_logs=test_path / "logs")


def test_configura_logs(test_path: Path, chave: str) -> None:
    assert isinstance(chave, str)
    assert os.path.exists(test_path / "logs")
    assert os.path.exists(test_path / f"logs/{chave}.log")

    logger = logging.getLogger(__name__)
    logger.info("Olá mundo!")

    with open(test_path / f"logs/{chave}.log") as f:
        texto = f.read()

    assert "Olá mundo!" in texto


def test_log_erros(test_path: Path, chave: str) -> None:
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

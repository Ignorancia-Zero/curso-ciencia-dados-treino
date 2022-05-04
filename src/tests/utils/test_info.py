import pandas as pd

from src.utils.info import carrega_excel
from src.utils.info import carrega_yaml


def test_carrega_yaml():
    info = carrega_yaml("aquis_censo_escolas.yml")

    assert isinstance(info, dict)
    assert "DADOS_SCHEMA" in info


def test_log_erros():
    info = carrega_excel("censo_escolar_etapa_ensino.xlsx")

    assert isinstance(info, pd.DataFrame)
    assert "TP_ETAPA_ENSINO" in info

from src.utils.info import carrega_yaml
from src.utils.info import carrega_excel
import pandas as pd


def test_carrega_yaml():
    info = carrega_yaml("aquis_censo_escolas.yml")

    assert isinstance(info, dict)
    assert "DADOS_SCHEMA" in info


def test_carrega_excel():
    info = carrega_excel("censo_escolar_etapa_ensino.xlsx")

    assert isinstance(info, pd.DataFrame)
    assert "TP_ETAPA_ENSINO" in info
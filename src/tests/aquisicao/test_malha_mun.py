import unittest
from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.ibge.malha_mun import MalhaMunIBGE


@pytest.fixture(scope="module")
def malha_etl(dados_path: Path, test_path: Path) -> MalhaMunIBGE:
    etl = MalhaMunIBGE(
        entrada=dados_path / "externo",
        saida=test_path,
        ano="2021",
        criar_caminho=False,
        reprocessar=False,
    )
    etl._links = {"BR_Municipios_2021.zip": ""}

    return etl


@pytest.mark.run(order=1)
def test_extract(malha_etl) -> None:
    malha_etl.extract()

    assert malha_etl.dados_entrada is not None
    assert len(malha_etl.dados_entrada) == 1
    assert {"2021"} == set(malha_etl.dados_entrada)
    for d in malha_etl.dados_entrada.values():
        assert isinstance(d, pd.DataFrame)


@pytest.mark.run(order=2)
def test_transform(malha_etl) -> None:
    malha_etl.transform()

    assert {
        "ANO",
        "CO_MUNICIPIO",
        "NO_MUN",
        "UF",
        "AREA_KM2",
        "geometry",
        "LATITUDE",
        "LONGITUDE",
    } == set(malha_etl.dados_saida["malha_mun.parquet"])


if __name__ == "__main__":
    unittest.main()

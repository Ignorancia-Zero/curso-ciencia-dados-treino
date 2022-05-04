import os
from pathlib import Path
import pandas as pd
import pytest

from src.aquisicao.inep.gestor import GestorETL


@pytest.fixture(scope="module")
def gestor_etl(dados_path: Path, test_path: Path, ano: int) -> GestorETL:
    etl = GestorETL(
        entrada=dados_path / "externo",
        saida=test_path,
        ano=ano,
        criar_caminho=False,
        reprocessar=False,
    )
    etl._inep = {k: "" for k in os.listdir(dados_path / f"externo/censo_escolar")}
    return etl


@pytest.mark.run(order=1)
def test_extract(gestor_etl: GestorETL, ano: int) -> None:
    gestor_etl.extract()

    assert gestor_etl.dados_entrada is not None
    assert len(gestor_etl.dados_entrada) == 1
    assert gestor_etl.ano == ano
    assert {f"{ano}"} == set(gestor_etl.dados_entrada)
    assert isinstance(gestor_etl.dados_entrada[f"{ano}"], pd.DataFrame)


@pytest.mark.run(order=2)
def test_gera_dt_nascimento(gestor_etl: GestorETL) -> None:
    base = gestor_etl.dados_entrada[f"{gestor_etl.ano}"]
    gestor_etl.gera_dt_nascimento(base)
    assert "DT_NASCIMENTO" in base
    assert base["DT_NASCIMENTO"].dtype == "datetime64[ns]"


@pytest.mark.run(order=3)
def test_processa_tp(gestor_etl: GestorETL) -> None:
    base = gestor_etl.dados_entrada[f"{gestor_etl.ano}"]
    gestor_etl.processa_tp(base)

    for c in gestor_etl._configs["DEPARA_TP"]:
        if c in base:
            assert "category" == base[c].dtype


@pytest.mark.run(order=4)
def test_remove_duplicatas(gestor_etl: GestorETL) -> None:
    base = gestor_etl.dados_entrada[f"{gestor_etl.ano}"]
    cols = set(base)

    base_id = gestor_etl.remove_duplicatas(base)

    assert base_id is not None
    depara_cols = set(["ID_GESTOR", "ANO"] + gestor_etl._configs["COLS_DEPARA"])
    assert depara_cols == set(base_id)
    assert cols - set(gestor_etl._configs["COLS_DEPARA"]) == set(base)

    gestor_etl.base_id = base_id


@pytest.mark.run(order=5)
def test_ajusta_schema(gestor_etl: GestorETL) -> None:
    base = gestor_etl.dados_entrada[f"{gestor_etl.ano}"]
    base = gestor_etl.ajusta_schema(
        base=base,
        fill=gestor_etl._configs["PREENCHER_NULOS"],
        schema=gestor_etl._configs["DADOS_SCHEMA"],
    )
    for c in gestor_etl._configs["PREENCHER_NULOS"]:
        if c in gestor_etl._configs["DADOS_SCHEMA"]:
            assert base.shape[0] == base[c].count(), f"{c}"
    assert set(base) == set(gestor_etl._configs["DADOS_SCHEMA"])
    for col, dtype in gestor_etl._configs["DADOS_SCHEMA"].items():
        if not dtype.startswith("pd."):
            assert base[col].dtype == dtype

    base_id = gestor_etl.base_id
    base_id = gestor_etl.ajusta_schema(
        base=base_id,
        fill=gestor_etl._configs["PREENCHER_NULOS"],
        schema=gestor_etl._configs["DEPARA_SCHEMA"],
    )
    for c in gestor_etl._configs["PREENCHER_NULOS"]:
        if c in gestor_etl._configs["DEPARA_SCHEMA"]:
            assert base_id.shape[0] == base_id[c].count()
    assert set(base_id) == set(gestor_etl._configs["DEPARA_SCHEMA"])
    for col, dtype in gestor_etl._configs["DEPARA_SCHEMA"].items():
        if not dtype.startswith("pd."):
            assert base_id[col].dtype == dtype

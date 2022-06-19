import os
from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.inep.docente import DocenteETL


@pytest.fixture(scope="module")
def docente_etl(dados_path: Path, test_path: Path, ano: int) -> DocenteETL:
    etl = DocenteETL(
        entrada=dados_path / "externo",
        saida=test_path,
        ano=ano,
        criar_caminho=False,
        reprocessar=False,
    )
    etl._inep = {k: "" for k in os.listdir(dados_path / f"externo/censo_escolar")}
    return etl


@pytest.mark.run(order=1)
def test_extract(docente_etl: DocenteETL, ano: int) -> None:
    docente_etl.extract()

    assert docente_etl.dados_entrada is not None
    assert len(docente_etl.dados_entrada) == 1
    assert docente_etl.ano == ano
    assert {f"{ano}"} == set(docente_etl.dados_entrada)
    assert isinstance(docente_etl.dados_entrada[f"{ano}"], pd.DataFrame)


@pytest.mark.run(order=2)
def test_gera_dt_nascimento(docente_etl: DocenteETL) -> None:
    base = docente_etl.dados_entrada[f"{docente_etl.ano}"]
    docente_etl.gera_dt_nascimento(base)
    assert "DT_NASCIMENTO" in base
    assert base["DT_NASCIMENTO"].dtype == "datetime64[ns]"


@pytest.mark.run(order=3)
def test_processa_in(docente_etl: DocenteETL) -> None:
    base = docente_etl.dados_entrada[f"{docente_etl.ano}"]
    docente_etl.processa_in(base)

    if "IN_INTERCULTURAL_OUTROS" in base and "IN_ESPECIFICO_OUTROS" not in base:
        assert "IN_ESPECIFICO_OUTROS" in base
        assert "IN_INTERCULTURAL_OUTROS" not in base


@pytest.mark.run(order=4)
def test_processa_tp(docente_etl: DocenteETL) -> None:
    base = docente_etl.dados_entrada[f"{docente_etl.ano}"]
    docente_etl.processa_tp(base)

    if "TP_ESCOLARIDADE" not in base and "TP_ESCOLARIDADE_0" in base:
        assert "TP_ESCOLARIDADE" in base
    if "TP_ENSINO_MEDIO" not in base and "TP_ESCOLARIDADE_0" in base:
        assert "TP_ENSINO_MEDIO" in base

    for c in docente_etl._configs["DEPARA_TP"]:
        if c in base:
            assert "category" == base[c].dtype


@pytest.mark.run(order=5)
def test_remove_duplicatas(docente_etl: DocenteETL) -> None:
    base = docente_etl.dados_entrada[f"{docente_etl.ano}"]
    cols = set(base)

    base_id = docente_etl.remove_duplicatas(base)

    assert base_id is not None
    depara_cols = set(["ID_DOCENTE", "ANO"] + docente_etl._configs["COLS_DEPARA"])
    assert depara_cols == set(base_id)
    assert cols - set(docente_etl._configs["COLS_DEPARA"]) == set(base)

    docente_etl.base_id = base_id  # type: ignore


@pytest.mark.run(order=6)
def test_ajusta_schema(docente_etl: DocenteETL) -> None:
    base = docente_etl.dados_entrada[f"{docente_etl.ano}"]
    base = docente_etl.ajusta_schema(
        base=base,
        fill=docente_etl._configs["PREENCHER_NULOS"],
        schema=docente_etl._configs["DADOS_SCHEMA"],
    )
    for c in docente_etl._configs["PREENCHER_NULOS"]:
        if c in docente_etl._configs["DADOS_SCHEMA"]:
            assert base.shape[0] == base[c].count(), f"{c}"
    assert set(base) == set(docente_etl._configs["DADOS_SCHEMA"])
    for col, dtype in docente_etl._configs["DADOS_SCHEMA"].items():
        if dtype == "str":
            assert base[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert base[col].dtype == dtype

    base_id = docente_etl.base_id  # type: ignore
    base_id = docente_etl.ajusta_schema(
        base=base_id,
        fill=docente_etl._configs["PREENCHER_NULOS"],
        schema=docente_etl._configs["DEPARA_SCHEMA"],
    )
    for c in docente_etl._configs["PREENCHER_NULOS"]:
        if c in docente_etl._configs["DEPARA_SCHEMA"]:
            assert base_id.shape[0] == base_id[c].count()
    assert set(base_id) == set(docente_etl._configs["DEPARA_SCHEMA"])
    for col, dtype in docente_etl._configs["DEPARA_SCHEMA"].items():
        if dtype == "str":
            assert base_id[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert base_id[col].dtype == dtype

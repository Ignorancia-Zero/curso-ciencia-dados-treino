import os
import random
from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.inep.matricula import MatriculaETL
from src.aquisicao.inep.matricula import _MatriculaRegiaoETL


@pytest.fixture(scope="module")
def matricula_reg_etl(
    dados_path: Path, test_path: Path, ano: int
) -> _MatriculaRegiaoETL:
    regiao = random.choice(["CO", "NORDESTE", "NORTE", "SUDESTE", "SUL"])

    etl = _MatriculaRegiaoETL(
        entrada=dados_path / "externo",
        saida=test_path,
        ano=ano,
        regiao=regiao,
        criar_caminho=False,
        reprocessar=False,
    )
    etl._inep = {k: "" for k in os.listdir(dados_path / f"externo/censo_escolar")}
    return etl


@pytest.fixture(scope="module")
def matricula_etl(dados_path: Path, test_path: Path, ano: int) -> MatriculaETL:
    etl = MatriculaETL(
        entrada=dados_path / "externo",
        saida=test_path,
        ano=ano,
        criar_caminho=False,
        reprocessar=False,
    )
    etl._inep = {k: "" for k in os.listdir(dados_path / f"externo/censo_escolar")}
    return etl


@pytest.mark.run(order=1)
def test_extract_reg(matricula_reg_etl: _MatriculaRegiaoETL, ano: int) -> None:
    matricula_reg_etl.extract()

    assert matricula_reg_etl.dados_entrada is not None
    assert len(matricula_reg_etl.dados_entrada) == 1
    assert matricula_reg_etl.ano == ano
    assert {f"{ano}"} == set(matricula_reg_etl.dados_entrada)
    assert isinstance(matricula_reg_etl.dados_entrada[f"{ano}"], pd.DataFrame)


@pytest.mark.run(order=2)
def test_gera_dt_nascimento(matricula_reg_etl: _MatriculaRegiaoETL) -> None:
    base = matricula_reg_etl.dados_entrada[f"{matricula_reg_etl.ano}"]
    matricula_reg_etl.gera_dt_nascimento(base)
    assert "DT_NASCIMENTO" in base
    assert base["DT_NASCIMENTO"].dtype == "datetime64[ns]"


@pytest.mark.run(order=3)
def test_processa_tp(matricula_reg_etl: _MatriculaRegiaoETL) -> None:
    base = matricula_reg_etl.dados_entrada[f"{matricula_reg_etl.ano}"]
    matricula_reg_etl.processa_tp(base)

    for c in matricula_reg_etl._configs["DEPARA_TP"]:
        if c in base:
            assert "category" == base[c].dtype


@pytest.mark.run(order=4)
def test_remove_duplicatas(matricula_reg_etl: _MatriculaRegiaoETL) -> None:
    base = matricula_reg_etl.dados_entrada[f"{matricula_reg_etl.ano}"]
    cols = set(base)

    base_id = matricula_reg_etl.remove_duplicatas(base)

    assert base_id is not None
    depara_cols = set(["ID_ALUNO", "ANO"] + matricula_reg_etl._configs["COLS_DEPARA"])
    assert depara_cols == set(base_id)
    assert cols - set(matricula_reg_etl._configs["COLS_DEPARA"]) == set(base)

    matricula_reg_etl.base_id = base_id


@pytest.mark.run(order=5)
def test_ajusta_schema(matricula_reg_etl: _MatriculaRegiaoETL) -> None:
    base = matricula_reg_etl.dados_entrada[f"{matricula_reg_etl.ano}"]
    base = matricula_reg_etl.ajusta_schema(
        base=base,
        fill=matricula_reg_etl._configs["PREENCHER_NULOS"],
        schema=matricula_reg_etl._configs["DADOS_SCHEMA"],
    )
    for c in matricula_reg_etl._configs["PREENCHER_NULOS"]:
        if c in matricula_reg_etl._configs["DADOS_SCHEMA"]:
            assert base.shape[0] == base[c].count(), f"{c}"
    assert set(base) == set(matricula_reg_etl._configs["DADOS_SCHEMA"])
    for col, dtype in matricula_reg_etl._configs["DADOS_SCHEMA"].items():
        if dtype == "str":
            assert base[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert base[col].dtype == dtype

    base_id = matricula_reg_etl.base_id
    base_id = matricula_reg_etl.ajusta_schema(
        base=base_id,
        fill=matricula_reg_etl._configs["PREENCHER_NULOS"],
        schema=matricula_reg_etl._configs["DEPARA_SCHEMA"],
    )
    for c in matricula_reg_etl._configs["PREENCHER_NULOS"]:
        if c in matricula_reg_etl._configs["DEPARA_SCHEMA"]:
            assert base_id.shape[0] == base_id[c].count()
    assert set(base_id) == set(matricula_reg_etl._configs["DEPARA_SCHEMA"])
    for col, dtype in matricula_reg_etl._configs["DEPARA_SCHEMA"].items():
        if dtype == "str":
            assert base_id[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert base_id[col].dtype == dtype


@pytest.mark.run(order=6)
def test_extract(matricula_etl: MatriculaETL, ano: int) -> None:
    matricula_etl.extract()

    assert matricula_etl.dados_entrada is not None
    assert len(matricula_etl.dados_entrada) == 0
    assert matricula_etl.ano == ano


@pytest.mark.run(order=7)
def test_transform(matricula_etl) -> None:
    matricula_etl.transform()

    for etl in matricula_etl._etls:
        assert len(etl.dados_saida) == 2
        for d in etl.dados_saida.values():
            assert isinstance(d, pd.DataFrame)

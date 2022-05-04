import os
from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.inep.turma import TurmaETL


@pytest.fixture(scope="module")
def turma_etl(dados_path: Path, test_path: Path, ano: int) -> TurmaETL:
    etl = TurmaETL(
        entrada=dados_path / "externo",
        saida=test_path,
        ano=ano,
        criar_caminho=False,
        reprocessar=False,
    )
    etl._inep = {k: "" for k in os.listdir(dados_path / f"externo/censo_escolar")}
    return etl


@pytest.mark.run(order=1)
def test_extract(turma_etl: TurmaETL, ano: int) -> None:
    turma_etl.extract()

    assert turma_etl.dados_entrada is not None
    assert len(turma_etl.dados_entrada) == 1
    assert turma_etl.ano == ano
    assert {f"{ano}"} == set(turma_etl.dados_entrada)
    assert isinstance(turma_etl.dados_entrada[f"{ano}"], pd.DataFrame)


@pytest.mark.run(order=2)
def test_processa_in(turma_etl: TurmaETL) -> None:
    base = turma_etl.dados_entrada[f"{turma_etl.ano}"]
    ad = {"IN_DISC_EST_SOCIAIS_SOCIOLOGIA", "IN_ESPECIAL_EXCLUSIVA"} - set(base)
    turma_etl.processa_in(base)
    if len(ad) > 0:
        assert ad.issubset(set(base))


@pytest.mark.run(order=3)
def test_processa_tp(turma_etl: TurmaETL) -> None:
    base = turma_etl.dados_entrada[f"{turma_etl.ano}"]
    ad = {"TP_MEDIACAO_DIDATICO_PEDAGO", "TP_TIPO_ATENDIMENTO_TURMA"} - set(base)
    turma_etl.processa_tp(base)
    if len(ad) > 0:
        assert ad.issubset(set(base))

    for c in turma_etl._configs["DEPARA_TP"]:
        if c in base:
            assert "category" == base[c].dtype


@pytest.mark.run(order=4)
def test_remove_duplicatas(turma_etl: TurmaETL) -> None:
    base = turma_etl.dados_entrada[f"{turma_etl.ano}"]
    base_id = turma_etl.remove_duplicatas(base)

    assert base_id is None


@pytest.mark.run(order=5)
def test_ajusta_schema(turma_etl: TurmaETL) -> None:
    base = turma_etl.dados_entrada[f"{turma_etl.ano}"]
    base = turma_etl.ajusta_schema(
        base=base,
        fill=turma_etl._configs["PREENCHER_NULOS"],
        schema=turma_etl._configs["DADOS_SCHEMA"],
    )

    for c in turma_etl._configs["PREENCHER_NULOS"]:
        assert base.shape[0] == base[c].count()

    assert set(base) == set(turma_etl._configs["DADOS_SCHEMA"])
    for col, dtype in turma_etl._configs["DADOS_SCHEMA"].items():
        if dtype == "str":
            assert base[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert base[col].dtype == dtype

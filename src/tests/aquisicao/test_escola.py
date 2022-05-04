import os
import re

import numpy as np
import pandas as pd
import pytest
from pathlib import Path

from src.aquisicao.inep.escola import EscolaETL


@pytest.fixture(scope="module")
def escola_etl(dados_path: Path, test_path: Path, ano: int) -> EscolaETL:
    etl = EscolaETL(
        entrada=dados_path / "externo",
        saida=test_path,
        ano=ano,
        criar_caminho=False,
        reprocessar=False,
    )
    etl._inep = {k: "" for k in os.listdir(dados_path / f"externo/censo_escolar")}

    return etl


@pytest.mark.run(order=1)
def test_extract(escola_etl: EscolaETL, ano: int) -> None:
    escola_etl.extract()

    assert escola_etl.dados_entrada is not None
    assert len(escola_etl.dados_entrada) == 1
    assert escola_etl.ano == ano
    assert {f"{ano}"} == set(escola_etl.dados_entrada)
    assert isinstance(escola_etl.dados_entrada[f"{ano}"], pd.DataFrame)


@pytest.mark.run(order=2)
def test_processa_dt(escola_etl: EscolaETL) -> None:
    base = escola_etl.dados_entrada[f"{escola_etl.ano}"]
    escola_etl.processa_dt(base)
    for c in base:
        if c.startswith("DT_"):
            assert base[c].dtype == "datetime64[ns]"


@pytest.mark.run(order=3)
def test_processa_qt(escola_etl: EscolaETL) -> None:
    base = escola_etl.dados_entrada[f"{escola_etl.ano}"]
    escola_etl.processa_qt(base)

    if escola_etl.ano >= 2019:
        for c in escola_etl._configs["COLS_88888"]:
            if c in base:
                assert 88888 not in base[c].values


@pytest.mark.run(order=4)
def test_processa_in(escola_etl: EscolaETL) -> None:
    base = escola_etl.dados_entrada[f"{escola_etl.ano}"]
    cols = set(base)
    criar_qt = set([c for c in base if c.startswith("QT_") and f"IN{c[2:]}" in cols])
    criar_comp = set(
        [
            criar
            for criar, t in escola_etl._configs["TRATAMENTO_IN"].items()
            for c in base
            if re.search(t[0], c)
        ]
    )

    escola_etl.processa_in(base)

    assert criar_qt.issubset(set(base))
    assert criar_comp.issubset(set(base))

    if "IN_ENERGIA_INEXISTENTE" in base:
        assert "IN_ENERGIA_OUTROS" in base
    if "TP_OCUPACAO_GALPAO" in base:
        assert "IN_LOCAL_FUNC_GALPAO" in base
    if "TP_INDIGENA_LINGUA" in base:
        assert "IN_LINGUA_INDIGENA" in base
        assert "IN_LINGUA_PORTUGUESA" in base
    if "IN_BIBLIOTECA" in base:
        assert "IN_BIBLIOTECA_SALA_LEITURA" in base
    if "IN_AGUA_FILTRADA" in base:
        assert "IN_AGUA_POTAVEL" in base

    for c in base:
        if c.startswith("IN_"):
            assert {0, 1, np.nan}, set(base[c].unique())


@pytest.mark.run(order=5)
def test_processa_tp(escola_etl: EscolaETL) -> None:
    base = escola_etl.dados_entrada[f"{escola_etl.ano}"]
    escola_etl.processa_tp(base)

    if "IN_LINGUA_INDIGENA" in base and "IN_LINGUA_PORTUGUESA" in base:
        assert "TP_INDIGENA_LINGUA" in base
        assert {
            "SEM EDUCAÇÃO INDÍGENA",
            "EM LÍNGUA INDÍGENA E EM LÍNGUA PORTUGUESA",
            "SOMENTE EM LÍNGUA INDÍGENA",
            "SOMENTE EM LÍNGUA PORTUGUESA",
        }.issuperset(set(base["TP_INDIGENA_LINGUA"].dropna().astype(str)))

    if "TP_OCUPACAO_GALPAO" in base:
        assert base["TP_OCUPACAO_GALPAO"].nunique() > 2
    if "TP_OCUPACAO_PREDIO_ESCOLAR" in base:
        assert base["TP_OCUPACAO_PREDIO_ESCOLAR"].nunique() > 2

    for c in escola_etl._configs["DEPARA_TP"]:
        if c in base:
            assert "category" == base[c].dtype


@pytest.mark.run(order=6)
def test_remove_duplicatas(escola_etl: EscolaETL) -> None:
    base = escola_etl.dados_entrada[f"{escola_etl.ano}"]
    base_id = escola_etl.remove_duplicatas(base)

    assert base_id is None


@pytest.mark.run(order=7)
def test_ajusta_schema(escola_etl: EscolaETL) -> None:
    base = escola_etl.dados_entrada[f"{escola_etl.ano}"]
    saida = escola_etl.ajusta_schema(
        base=base,
        fill=escola_etl._configs["PREENCHER_NULOS"],
        schema=escola_etl._configs["DADOS_SCHEMA"],
    )

    for c in escola_etl._configs["PREENCHER_NULOS"]:
        assert saida.shape[0] == saida[c].count()

    assert set(saida) == set(escola_etl._configs["DADOS_SCHEMA"])
    for col, dtype in escola_etl._configs["DADOS_SCHEMA"].items():
        if dtype == "str":
            assert saida[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert saida[col].dtype == dtype

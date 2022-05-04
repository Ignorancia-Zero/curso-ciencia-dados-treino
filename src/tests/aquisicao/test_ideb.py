import os
from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.inep.ideb import IDEBETL


@pytest.fixture(scope="module")
def ideb_etl(dados_path: Path, test_path: Path) -> IDEBETL:
    etl = IDEBETL(
        entrada=dados_path / "externo",
        saida=test_path,
        criar_caminho=False,
        reprocessar=False,
    )
    etl._links = {k: "" for k in sorted(os.listdir(dados_path / f"externo/ideb"))}

    return etl


@pytest.fixture(scope="module")
def data() -> dict:
    return dict()


@pytest.mark.run(order=1)
def test_extract(ideb_etl: IDEBETL) -> None:
    ideb_etl.extract()

    assert ideb_etl.dados_entrada is not None
    assert len(ideb_etl.dados_entrada) == 3
    assert {
        "divulgacao_anos_finais_escolas_2019.zip",
        "divulgacao_anos_iniciais_escolas_2019.zip",
        "divulgacao_ensino_medio_escolas_2019.zip",
    } == set(ideb_etl.dados_entrada)
    for d in ideb_etl.dados_entrada.values():
        assert isinstance(d, pd.DataFrame)


@pytest.mark.run(order=2)
def test_extrai_turma(ideb_etl: IDEBETL) -> None:
    assert ideb_etl.extrai_turma("divulgacao_anos_finais_escolas_2019.zip") == "AF"
    assert ideb_etl.extrai_turma("divulgacao_anos_iniciais_escolas_2019.zip") == "AI"
    assert ideb_etl.extrai_turma("divulgacao_ensino_medio_escolas_2019.zip") == "EM"


@pytest.mark.run(order=3)
def test_seleciona_dados(ideb_etl: IDEBETL, data: dict) -> None:
    df = ideb_etl.dados_entrada["divulgacao_anos_finais_escolas_2019.zip"]
    data["df"] = ideb_etl.seleciona_dados(df)
    assert (
        len(
            {"SG_UF", "CO_MUNICIPIO", "NO_MUNICIPIO", "NO_ESCOLA", "REDE"}.intersection(
                set(data["df"].columns)
            )
        )
        == 0
    )
    assert data["df"].shape[0] == df.shape[0] - 3


@pytest.mark.run(order=4)
def test_obtem_metricas(ideb_etl: IDEBETL, data: dict) -> None:
    data["dados"] = ideb_etl.obtem_metricas(data["df"], "AF")

    assert set(data["df"].iloc[:, 1:].columns) == set(data["dados"]["COLUNA"])
    assert set(range(2005, 2022, 2)) == set(data["dados"]["ANO"])
    assert {
        "APROVACAO_AF",
        "APROVACAO_AF_1",
        "APROVACAO_AF_2",
        "APROVACAO_AF_3",
        "APROVACAO_AF_4",
        "IDEB_AF",
        "IDEB_META_AF",
        "NOTA_MATEMATICA_AF",
        "NOTA_MEDIA_AF",
        "NOTA_PORTUGUES_AF",
        "REND_AF",
    } == set(data["dados"]["METRICA"])


@pytest.mark.run(order=5)
def test_formata_resultados(ideb_etl: IDEBETL, data: dict) -> None:
    data["df"] = ideb_etl.formata_resultados(data["df"], data["dados"])

    assert set(["ID_ESCOLA", "ANO"] + list(data["dados"]["METRICA"].unique())) == set(
        data["df"].columns
    )
    assert all(
        [
            "float32" == data["df"][c].dtype
            for c in data["df"]
            if c != "ID_ESCOLA" and c != "ANO"
        ]
    )


@pytest.mark.run(order=6)
def test_concatena_saidas(ideb_etl, data) -> None:
    df2 = ideb_etl.seleciona_dados(
        ideb_etl.dados_entrada["divulgacao_anos_iniciais_escolas_2019.zip"]
    )
    dados = ideb_etl.obtem_metricas(df2, "AI")
    df2 = ideb_etl.formata_resultados(df2, dados)

    df3 = ideb_etl.seleciona_dados(
        ideb_etl.dados_entrada["divulgacao_ensino_medio_escolas_2019.zip"]
    )
    dados = ideb_etl.obtem_metricas(df3, "EM")
    df3 = ideb_etl.formata_resultados(df3, dados)

    res = ideb_etl.concatena_saidas([data["df"], df2, df3])

    assert {
        "ID_ESCOLA",
        "ANO",
        "APROVACAO_AI",
        "APROVACAO_AI_1",
        "APROVACAO_AI_2",
        "APROVACAO_AI_3",
        "APROVACAO_AI_4",
        "IDEB_AI",
        "IDEB_META_AI",
        "NOTA_MATEMATICA_AI",
        "NOTA_MEDIA_AI",
        "NOTA_PORTUGUES_AI",
        "REND_AI",
        "APROVACAO_AF",
        "APROVACAO_AF_1",
        "APROVACAO_AF_2",
        "APROVACAO_AF_3",
        "APROVACAO_AF_4",
        "IDEB_AF",
        "IDEB_META_AF",
        "NOTA_MATEMATICA_AF",
        "NOTA_MEDIA_AF",
        "NOTA_PORTUGUES_AF",
        "REND_AF",
        "APROVACAO_EM",
        "APROVACAO_EM_1",
        "APROVACAO_EM_2",
        "APROVACAO_EM_3",
        "APROVACAO_EM_4",
        "IDEB_EM",
        "IDEB_META_EM",
        "NOTA_MATEMATICA_EM",
        "NOTA_MEDIA_EM",
        "NOTA_PORTUGUES_EM",
        "REND_EM",
    } == set(res.columns)

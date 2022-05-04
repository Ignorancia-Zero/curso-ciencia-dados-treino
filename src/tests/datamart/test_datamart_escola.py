import typing
from pathlib import Path

import pandas as pd
import pytest

import src.datamart.escola as dm_escola
from src.utils.info import carrega_excel


@pytest.fixture(scope="module")
def dados() -> typing.Dict[str, pd.DataFrame]:
    return dict(dm=pd.DataFrame())


def test_processa_censo_escola(
    dados: typing.Dict[str, pd.DataFrame], dados_path: Path, ano: int
) -> None:
    dados["dm"] = dm_escola.processa_censo_escola(
        aquis_entrada=dados_path / "externo",
        aquis_saida=dados_path / "aquisicao",
        ano=ano,
    )

    assert set(dados["dm"]["ANO"]) == {ano}
    assert 0 < dados["dm"].shape[0] <= 1000


def test_processa_turmas(
    dados: typing.Dict[str, pd.DataFrame], dados_path: Path, ano: int
) -> None:
    turma = pd.read_parquet(
        dados_path / "aquisicao/turma.parquet",
        filters=[("ANO", "=", ano)],
    )
    dados["dm"] = dm_escola.processa_turmas(
        dm=dados["dm"],
        aquis_entrada=dados_path / "externo",
        aquis_saida=dados_path / "aquisicao",
        ano=ano,
    )

    assert "QT_TURMAS" in dados["dm"].columns
    assert "PC_TURMA_ESPECIAL_EXCLUSIVA" in dados["dm"].columns

    in_cols = [f"IN_TURMA_{c[3:]}" for c in turma if c.startswith("IN_")]
    in_cols += [c.replace("IN_TURMA_", "QT_TURMA_") for c in in_cols]
    assert set(in_cols).issubset(set(dados["dm"].columns))

    for (tp_col, pf) in [
        ("TP_MEDIACAO_DIDATICO_PEDAGO", "TURMA_MEDIACAO"),
        ("TP_TIPO_ATENDIMENTO_TURMA", "TURMA_ATEND"),
        ("TP_TIPO_LOCAL_TURMA", "TURMA_LOCAL"),
        ("TP_MOD_ENSINO", "TURMA_MOD"),
        ("TP_TIPO_TURMA", "TURMA_TIPO"),
    ]:
        assert tp_col in dados["dm"].columns
        for cat in turma[tp_col].dtype.categories:
            cat = cat.replace(" ", "_").replace("-", "_").replace("___", "_")
            assert f"QT_{pf}_{cat}" in dados["dm"].columns
            assert f"PC_{pf}_{cat}" in dados["dm"].columns
        assert f"QT_{pf}_NULO" in dados["dm"].columns

    assert "PC_TURMA_ESPECIAL_EXCLUSIVA" in dados["dm"].columns
    assert "NU_TURMA_MEAN_DURACAO" in dados["dm"].columns
    assert "NU_TURMA_MEDIAN_DURACAO" in dados["dm"].columns
    assert "QT_TURMA_ATIVIDADE_COMP" in dados["dm"].columns
    for tipo in [
        "REGULAR",
        "INFANTIL",
        "FUNDAMENTAL",
        "AI",
        "AF",
        "MEDIO",
        "EJA",
        "PROFISSIONALIZANTE",
        "TECNICO",
        "FIC",
    ]:
        assert f"QT_TURMA_{tipo}" in dados["dm"].columns
        assert f"IN_TURMA_{tipo}" in dados["dm"].columns


def test_processa_docentes(
    dados: typing.Dict[str, pd.DataFrame], dados_path: Path, ano: int
):
    dados["dm"] = dm_escola.processa_docentes(
        dm=dados["dm"],
        aquis_entrada=dados_path / "externo",
        aquis_saida=dados_path / "aquisicao",
        ano=ano,
    )

    docente = pd.read_parquet(
        dados_path / "aquisicao/docente.parquet",
        filters=[("ANO", "=", ano)],
    )
    depara = (
        pd.read_parquet(
            dados_path / "aquisicao/depara_docente_turma.parquet",
            filters=[("ANO", "=", ano)],
        )
        .merge(
            pd.read_parquet(
                dados_path / "aquisicao/turma.parquet",
                filters=[("ANO", "=", ano)],
                columns=["ID_TURMA", "ID_ESCOLA"],
            )
        )
        .drop(columns=["ID_TURMA"])
        .drop_duplicates()
    )

    assert "QT_DOCENTES" in dados["dm"].columns

    in_cols = [f"IN_DOCENTE_{c[3:]}" for c in docente if c.startswith("IN_")]
    in_cols += [c.replace("IN_DOCENTE_", "QT_DOCENTE_") for c in in_cols] + [
        c.replace("IN_DOCENTE_", "PC_DOCENTE_") for c in in_cols
    ]
    assert set(in_cols).issubset(set(dados["dm"].columns))

    for (tp_col, pf, df) in [
        ("TP_SEXO", "DOCENTE_SEXO", docente),
        ("TP_COR_RACA", "DOCENTE_COR", docente),
        ("TP_NACIONALIDADE", "DOCENTE_NASC", docente),
        ("TP_ZONA_RESIDENCIAL", "DOCENTE_ZONA", docente),
        ("TP_LOCAL_RESID_DIFERENCIADA", "DOCENTE_LDIF", docente),
        ("TP_ESCOLARIDADE", "DOCENTE_ESC", docente),
        ("TP_ENSINO_MEDIO", "DOCENTE_EM", docente),
        ("TP_TIPO_CONTRATACAO", "DOCENTE_CONT", depara),
        ("TP_TIPO_DOCENTE", "DOCENTE_TIPO", depara),
    ]:
        for cat in df[tp_col].dtype.categories:
            cat = cat.replace(" ", "_").replace("-", "_").replace("___", "_")
            assert f"QT_{pf}_{cat}" in dados["dm"].columns
            assert f"PC_{pf}_{cat}" in dados["dm"].columns
        assert f"QT_{pf}_NULO" in dados["dm"].columns

    assert "QT_DOCENTE_COMPL_PEDAGOGICA" in dados["dm"].columns
    assert "QT_DOCENTE_MUN_DIF" in dados["dm"].columns


def test_processa_gestor(
    dados: typing.Dict[str, pd.DataFrame], dados_path: Path, ano: int
):
    dados["dm"] = dm_escola.processa_gestor(
        dm=dados["dm"],
        aquis_entrada=dados_path / "externo",
        aquis_saida=dados_path / "aquisicao",
        ano=ano,
    )

    df_cr = carrega_excel("censo_escolar_cursos.xlsx")
    df_cr["TP_GRAU_ACADEMICO"] = df_cr["TP_GRAU_ACADEMICO"].astype("category")
    df_cr["TP_AREA_CURSO"] = df_cr["TP_AREA_CURSO"].astype("category")
    gestor = (
        pd.read_parquet(
            dados_path / "aquisicao/gestor.parquet",
            filters=[("ANO", "=", ano)],
        )
        .merge(df_cr, left_on=["CO_CURSO_1"], right_on=["CO_CURSO"], how="left")
        .drop(columns=["CO_CURSO"])
    )
    depara = pd.read_parquet(
        dados_path / "aquisicao/depara_gestor_escola.parquet",
        filters=[("ANO", "=", ano)],
    )

    in_cols = [f"IN_GESTOR_{c[3:]}" for c in gestor if c.startswith("IN_")]
    in_cols += [c.replace("IN_GESTOR_", "QT_GESTOR_") for c in in_cols]
    assert set(in_cols).issubset(set(dados["dm"].columns))

    for (tp_col, pf, df) in [
        ("TP_SEXO", "GESTOR_SEXO", gestor),
        ("TP_COR_RACA", "GESTOR_COR", gestor),
        ("TP_NACIONALIDADE", "GESTOR_NASC", gestor),
        ("TP_ESCOLARIDADE", "GESTOR_ESC", gestor),
        ("TP_ENSINO_MEDIO", "GESTOR_EM", gestor),
        ("TP_AREA_CURSO", "GESTOR_FORMACAO", gestor),
        ("TP_CARGO_GESTOR", "GESTOR_CARGO", depara),
        ("TP_TIPO_ACESSO_CARGO", "GESTOR_ACESSO", depara),
        ("TP_TIPO_CONTRATACAO", "GESTOR_CONT", depara),
    ]:
        for cat in df[tp_col].dtype.categories:
            cat = cat.replace(" ", "_").replace("-", "_").replace("___", "_")
            assert f"QT_{pf}_{cat}" in dados["dm"].columns
            assert f"PC_{pf}_{cat}" in dados["dm"].columns
        assert f"QT_{pf}_NULO" in dados["dm"].columns


def test_processa_matricula(
    dados: typing.Dict[str, pd.DataFrame], dados_path: Path, ano: int
):
    dados["dm"] = dm_escola.processa_matricula(
        dm=dados["dm"],
        aquis_entrada=dados_path / "externo",
        aquis_saida=dados_path / "aquisicao",
        ano=ano,
    )

    aluno = pd.read_parquet(
        dados_path / "aquisicao/aluno.parquet",
        filters=[("ANO", "=", ano)],
    )
    matricula = pd.read_parquet(
        dados_path / "aquisicao/matricula.parquet",
        filters=[("ANO", "=", ano)],
    )

    in_cols = [f"IN_ALUNO_{c[3:]}" for c in aluno if c.startswith("IN_")] + [
        f"IN_ALUNO_{c[3:]}" for c in matricula.columns if c.startswith("IN_TRANSP")
    ]
    in_cols += [c.replace("IN_ALUNO_", "QT_ALUNO_") for c in in_cols] + [
        c.replace("IN_ALUNO_", "PC_ALUNO_") for c in in_cols
    ]
    m_in = [
        f"IN_MATRICULA_{c[3:]}" for c in matricula.columns if c.startswith("IN_AEE")
    ]
    m_in += [c.replace("IN_MATRICULA_", "QT_MATRICULA_") for c in m_in] + [
        c.replace("IN_MATRICULA_", "PC_MATRICULA_") for c in m_in
    ]
    in_cols += m_in
    assert set(in_cols).issubset(set(dados["dm"].columns))

    for (tp_col, pf, df) in [
        ("TP_SEXO", "ALUNO_SEXO", aluno),
        ("TP_COR_RACA", "ALUNO_COR", aluno),
        ("TP_NACIONALIDADE", "ALUNO_NASC", aluno),
        ("TP_ZONA_RESIDENCIAL", "ALUNO_ZONA", aluno),
        ("TP_LOCAL_RESID_DIFERENCIADA", "ALUNO_LDIF", aluno),
        ("TP_INGRESSO_FEDERAIS", "ALUNO_INGRESSO", aluno),
        ("TP_RESPONSAVEL_TRANSPORTE", "RESP_TRANSP", matricula),
    ]:
        for cat in df[tp_col].dtype.categories:
            cat = cat.replace(" ", "_").replace("-", "_").replace("___", "_")
            assert f"QT_{pf}_{cat}" in dados["dm"].columns
            assert f"PC_{pf}_{cat}" in dados["dm"].columns
        assert f"QT_{pf}_NULO" in dados["dm"].columns

    assert "QT_ALUNO_MUN_DIF" in dados["dm"].columns

    nu_qt_cols = [
        f"{c[:2]}_{m.upper()}_ALUNO_{c[3:]}"
        for c in aluno
        for m in ["min", "q1", "mean", "median", "q3", "max"]
        if c.startswith("QT_") or c.startswith("NU_")
    ]
    assert set(nu_qt_cols).issubset(set(dados["dm"].columns))


def test_processa_ideb(
    dados: typing.Dict[str, pd.DataFrame], dados_path: Path, ano: int
):
    dados["dm"] = dm_escola.processa_ideb(
        dm=dados["dm"],
        aquis_entrada=dados_path / "externo",
        aquis_saida=dados_path / "aquisicao",
        ano=ano,
    )

    ideb = pd.read_parquet(
        dados_path / "aquisicao/ideb.parquet",
        filters=[("ANO", "=", ano)],
    ).drop(columns="ANO")
    assert set(ideb.columns).issubset(dados["dm"].columns)


def test_gera_metricas_adicionais(dados: typing.Dict[str, pd.DataFrame]):
    dados["dm"] = dm_escola.gera_metricas_adicionais(dados["dm"])

    assert {
        "NU_COMP_PORTATIL_ALUNO",
        "NU_DESKTOP_TOTAL_POR_ALUNO",
        "NU_DESKTOP_ADM_POR_ALUNO",
        "NU_DESKTOP_ALUNO_POR_ALUNO",
        "NU_TABLET_POR_ALUNO",
        "NU_EQUIP_COPIADORA_POR_ALUNO",
        "NU_EQUIP_DVD_POR_ALUNO",
        "NU_EQUIP_FAX_POR_ALUNO",
        "NU_EQUIP_FOTO_POR_ALUNO",
        "NU_EQUIP_IMPRESSORA_POR_ALUNO",
        "NU_EQUIP_IMPRESSORA_MULT_POR_ALUNO",
        "NU_EQUIP_LOUSA_DIGITAL_POR_ALUNO",
        "NU_EQUIP_MULTIMIDIA_POR_ALUNO",
        "NU_EQUIP_PARABOLICA_POR_ALUNO",
        "NU_EQUIP_RETRO_POR_ALUNO",
        "NU_EQUIP_SOM_POR_ALUNO",
        "NU_EQUIP_TV_POR_ALUNO",
        "NU_EQUIP_VIDEOCASSETE_POR_ALUNO",
        "NU_FUNCIONARIOS_POR_ALUNO",
        "NU_SALAS_POR_ALUNO",
        "NU_SALAS_UTILIZADAS_POR_ALUNO",
        "NU_SALAS_UTILIZADAS_ACESSIVEIS_POR_ALUNO",
        "NU_SALAS_UTILIZADAS_DENTRO_POR_ALUNO",
        "NU_SALAS_UTILIZADAS_FORA_POR_ALUNO",
        "NU_SALAS_UTILIZA_CLIMATIZADAS_POR_ALUNO",
        "NU_ALUNO_POR_TURMA",
        "NU_ALUNO_POR_DOCENTE",
        "CO_REGIAO",
        "CO_UF",
    }.issubset(dados["dm"].columns)

import typing
from pathlib import Path

import numpy as np
import pandas as pd

from src.aquisicao.inep._censo import _BaseCensoEscolarETL


class EscolaETL(_BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de escola
    do censo escolar
    """

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL de dados de Escola

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            tabela="escolas",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

    @property
    def bases_saida(self) -> typing.List[str]:
        return ["escola.parquet"]

    def processa_in(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas indicadoras

        :param base: base de dados a ser processada
        """
        super(EscolaETL, self).processa_in(base)

        # cria a coluna IN_ENERGIA_OUTROS
        if "IN_ENERGIA_OUTROS" not in base and "IN_ENERGIA_INEXISTENTE" in base:
            base["IN_ENERGIA_OUTROS"] = (
                (
                    base[
                        [
                            c
                            for c in base
                            if "IN_ENERGIA" in c and c != "IN_ENERGIA_INEXISTENTE"
                        ]
                    ].sum(axis=1)
                    == 0
                )
                & (base["IN_ENERGIA_INEXISTENTE"] == 0)
            ).astype("int")

        # cria a coluna IN_LOCAL_FUNC_GALPAO
        if "IN_LOCAL_FUNC_GALPAO" not in base and "TP_OCUPACAO_GALPAO" in base:
            base["IN_LOCAL_FUNC_GALPAO"] = np.where(
                (base["TP_OCUPACAO_GALPAO"] > 0) & (base["TP_OCUPACAO_GALPAO"] <= 3),
                1,
                np.where(base["TP_OCUPACAO_GALPAO"] == 0, 0, np.nan),
            )

        # cria a coluna IN_LINGUA_INDIGENA e IN_LINGUA_PORTUGUESA
        if "IN_LINGUA_INDIGENA" not in base and "TP_INDIGENA_LINGUA" in base:
            base["IN_LINGUA_INDIGENA"] = (
                base["TP_INDIGENA_LINGUA"].isin([1, 3])
            ).astype("int")

        if "IN_LINGUA_PORTUGUESA" not in base and "TP_INDIGENA_LINGUA" in base:
            base["IN_LINGUA_PORTUGUESA"] = (
                base["TP_INDIGENA_LINGUA"].isin([2, 3])
            ).astype("int")

        # corrige a coluna IN_BIBLIOTECA
        if "IN_SALA_LEITURA" not in base and "IN_BIBLIOTECA" in base:
            if "IN_BIBLIOTECA_SALA_LEITURA" in base:
                base.drop(columns=["IN_BIBLIOTECA_SALA_LEITURA"], inplace=True)
            base.rename(
                columns={"IN_BIBLIOTECA": "IN_BIBLIOTECA_SALA_LEITURA"},
                inplace=True,
            )

        # cria a coluna IN_AGUA_POTAVEL
        if "IN_AGUA_POTAVEL" not in base and "IN_AGUA_FILTRADA" in base:
            base["IN_AGUA_POTAVEL"] = ((base["IN_AGUA_FILTRADA"] == 2)).astype("int")

        # substítui o valor 9 pelo valor nulo
        for c in base:
            if c.startswith("IN_"):
                base[c] = base[c].replace({9: np.nan})

    def processa_tp(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: base de dados a ser processada
        """
        # cria a coluna TP_INDIGENA_LINGUA
        if (
            "TP_INDIGENA_LINGUA" not in base
            and "IN_LINGUA_INDIGENA" in base
            and "IN_LINGUA_PORTUGUESA" in base
        ):
            base["TP_INDIGENA_LINGUA"] = np.where(
                (base["IN_LINGUA_INDIGENA"] == 1) & (base["IN_LINGUA_PORTUGUESA"] == 0),
                1,
                np.where(
                    (base["IN_LINGUA_INDIGENA"] == 0)
                    & (base["IN_LINGUA_PORTUGUESA"] == 1),
                    2,
                    np.where(
                        (base["IN_LINGUA_INDIGENA"] == 1)
                        & (base["IN_LINGUA_PORTUGUESA"] == 1),
                        3,
                        np.where(
                            (
                                base["TP_SITUACAO_FUNCIONAMENTO"]
                                .astype("str")
                                .isin(["1", "1.0", "EM ATIVIDADE"])
                            )
                            & (base["IN_EDUCACAO_INDIGENA"] == 0),
                            0,
                            np.nan,
                        ),
                    ),
                ),
            )

        # Corrige o campo de TP_OCUPACAO_GALPAO
        if "TP_OCUPACAO_GALPAO" in base:
            if base["TP_OCUPACAO_GALPAO"].max() == 1:
                base.drop(columns=["TP_OCUPACAO_GALPAO"], inplace=True)

        # Corrige o campo de TP_OCUPACAO_PREDIO_ESCOLAR
        if "TP_OCUPACAO_PREDIO_ESCOLAR" in base:
            if base["TP_OCUPACAO_PREDIO_ESCOLAR"].max() == 1:
                base.drop(columns=["TP_OCUPACAO_PREDIO_ESCOLAR"], inplace=True)

        # converte a coluna para tipo categórico
        super(EscolaETL, self).processa_tp(base)

        # vamos converter as variáveis indicadores de escolas particulares
        # em variáveis TP, uma vez que teremos uma terceira opção informando
        # que a escola é pública
        for c in self._configs["COLS_PARTICULAR"]:
            if c in base:
                base.rename(columns={c: f"TP{c[2:]}"}, inplace=True)
                base[f"TP{c[2:]}"] = base[f"TP{c[2:]}"].replace({0: "NÃO", 1: "SIM"})
                base[f"TP{c[2:]}"] = np.where(
                    (base["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                    & (base["TP_DEPENDENCIA"] == "PRIVADA"),
                    base[f"TP{c[2:]}"],
                    np.where(
                        (base["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                        & ~(base["TP_DEPENDENCIA"] == "PRIVADA"),
                        base[f"TP{c[2:]}"].fillna("PÚBLICA"),
                        np.nan,
                    ),
                )
                base[f"TP{c[2:]}"] = base[f"TP{c[2:]}"].astype("category")

    def ajusta_schema(
        self,
        base: pd.DataFrame,
        fill: typing.Dict[str, typing.Any],
        schema: typing.Dict[str, str],
    ) -> pd.DataFrame:
        """
        Modifica o schema de uma base para bater com as configurações

        :param base: base de dados a ser processada
        :param fill: dicionário de preenchimento por coluna
        :param schema: dicionário de tipo de dados por coluna
        """
        # corrige variáveis de escolas privadas
        for c in self._configs["COLS_PARTICULAR"]:
            c = c if c.startswith("TP") else f"TP{c[2:]}"
            if c in base:
                base[c] = np.where(
                    (base[c] == "PÚBLICA") & (base["TP_DEPENDENCIA"] == "PRIVADA"),
                    np.nan,
                    base[c],
                )

        # corrige a ocupação de galpão
        if "TP_OCUPACAO_GALPAO" in base:
            # corrige a ocupação de galpão
            base["TP_OCUPACAO_GALPAO"] = np.where(
                (
                    (base["TP_OCUPACAO_GALPAO"] == "NÃO")
                    & (base["IN_LOCAL_FUNC_GALPAO"] == 1)
                )
                | (
                    (base["TP_OCUPACAO_GALPAO"] != "NÃO")
                    & (base["IN_LOCAL_FUNC_GALPAO"] == 0)
                ),
                np.nan,
                base["TP_OCUPACAO_GALPAO"],
            )

        # garante que todas as colunas existam
        return super(EscolaETL, self).ajusta_schema(base, fill, schema)

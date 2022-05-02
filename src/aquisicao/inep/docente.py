import typing
from pathlib import Path

import numpy as np
import pandas as pd

from src.aquisicao.inep._censo import _BaseCensoEscolarETL


class DocenteETL(_BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de docente do
    censo escolar
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
        Instância o objeto de ETL de dados de Docente

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            tabela="docentes",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

    @property
    def bases_saida(self) -> typing.List[str]:
        return ["docente.parquet", "depara_docente_turma.parquet"]

    def processa_in(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas indicadoras

        :param base: base de dados a ser processada
        """
        super(DocenteETL, self).processa_in(base)

        if "IN_INTERCULTURAL_OUTROS" in base and "IN_ESPECIFICO_OUTROS" not in base:
            base.rename(
                columns={"IN_INTERCULTURAL_OUTROS": "IN_ESPECIFICO_OUTROS"},
                inplace=True,
            )

    def processa_tp(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: base de dados a ser processada
        """
        if "TP_ESCOLARIDADE" not in base and "TP_ESCOLARIDADE_0" in base:
            base["TP_ESCOLARIDADE"] = np.where(
                base["TP_ESCOLARIDADE_0"] <= 2,
                base["TP_ESCOLARIDADE_0"],
                np.where(
                    base["TP_ESCOLARIDADE_0"].isin([3, 4, 5]),
                    3,
                    np.where(base["TP_ESCOLARIDADE_0"] == 6, 4, np.nan),
                ),
            )

        if "TP_ENSINO_MEDIO" not in base and "TP_ESCOLARIDADE_0" in base:
            base["TP_ENSINO_MEDIO"] = np.where(
                base["TP_ESCOLARIDADE_0"] == 5,
                1,
                np.where(
                    base["TP_ESCOLARIDADE_0"] == 3,
                    2,
                    np.where(base["TP_ESCOLARIDADE_0"] == 4, 4, 9),
                ),
            )

        if "TP_TIPO_DOCENTE" in base:
            if base["TP_TIPO_DOCENTE"].min() == 0:
                base["TP_TIPO_DOCENTE"] += 1

        super(DocenteETL, self).processa_tp(base)

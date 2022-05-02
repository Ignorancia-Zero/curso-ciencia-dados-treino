import typing
from pathlib import Path

import numpy as np
import pandas as pd

from src.aquisicao.inep._censo import _BaseCensoEscolarETL


class TurmaETL(_BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de turma do
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
        Instância o objeto de ETL de dados de Turma

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            tabela="turmas",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

    @property
    def bases_saida(self) -> typing.List[str]:
        return ["turma.parquet"]

    def processa_in(self, base: pd.DataFrame) -> None:
        """
        Gera colunas IN_ que não existiam em bases mais antigas

        :param base: base de dados a ser processada
        """
        super(TurmaETL, self).processa_in(base)

        if "IN_ESPECIAL_EXCLUSIVA" not in base:
            if "TP_MOD_ENSINO" in base:
                base["IN_ESPECIAL_EXCLUSIVA"] = (base["TP_MOD_ENSINO"] == 2).astype(
                    "int"
                )

    def processa_tp(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: base de dados a ser processada
        """
        # gera a coluna de TP_MEDIACAO_DIDATICO_PEDAGO
        if "TP_MEDIACAO_DIDATICO_PEDAGO" not in base:
            base["TP_MEDIACAO_DIDATICO_PEDAGO"] = np.where(
                base["CO_ETAPA_ENSINO"].isin([46, 47, 48, 53, 54, 55, 58, 61, 63]),
                2,
                np.where(base["CO_ETAPA_ENSINO"].notnull(), 1, np.nan),
            )

        # gera a coluna de TP_TIPO_ATENDIMENTO_TURMA
        if "TP_TIPO_ATENDIMENTO_TURMA" not in base:
            if "TP_MOD_ENSINO" in base:
                regular = base["TP_MOD_ENSINO"] == 1
            elif "IN_REGULAR" in base:
                regular = base["IN_REGULAR"] == 1
            else:
                regular = None

            if "NU_DIAS_ATIVIDADE" in base:
                ae = base["NU_DIAS_ATIVIDADE"].isin([1, 2, 3, 4, 5, 6, 7])
            elif "CO_TIPO_ATIVIDADE_1" in base:
                ae = base["CO_TIPO_ATIVIDADE_1"].notnull()
            elif "TP_TIPO_TURMA" in base:
                ae = base["TP_TIPO_TURMA"] == 4
            else:
                ae = None

            if "TP_MOD_ENSINO" in base:
                especial = base["TP_MOD_ENSINO"] == 2
            elif "IN_ESPECIAL_EXCLUSIVA" in base:
                especial = base["IN_ESPECIAL_EXCLUSIVA"] == 1
            elif "IN_DISC_ATENDIMENTO_ESPECIAIS" in base:
                especial = base["IN_DISC_ATENDIMENTO_ESPECIAIS"] == 1
            elif "TP_TIPO_TURMA" in base and base["TP_TIPO_TURMA"].max() >= 5:
                especial = base["TP_TIPO_TURMA"] == 5
            else:
                especial = None

            if (regular is not None) and (ae is not None) and (especial is not None):
                base["TP_TIPO_ATENDIMENTO_TURMA"] = np.where(
                    especial,
                    4,
                    np.where(
                        regular & ae, 2, np.where(ae, 3, np.where(regular, 1, np.nan))
                    ),
                )

        # converte a coluna para tipo categórico
        super(TurmaETL, self).processa_tp(base)

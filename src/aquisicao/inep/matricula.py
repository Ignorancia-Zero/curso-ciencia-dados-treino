import os
import typing
from pathlib import Path

import pandas as pd

from src.aquisicao.inep._censo import _BaseCensoEscolarETL


class _MatriculaRegiaoETL(_BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de matrícula do
    censo escolar para uma região em particular

    Este objeto é construído pelo MatriculaETL e utilizado para
    fazer uma gestão eficiente de memória
    """

    reg: str

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        regiao: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL de dados de Matrícula

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param regiao: região a ser processada
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            tabela="matricula",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
            regioes=[regiao],
        )
        self.reg = regiao.upper()

    @property
    def bases_saida(self) -> typing.List[str]:
        return ["aluno.parquet", "matricula.parquet"]

    def carrega_saidas(self) -> None:
        """
        Carrega os dados de saída no dicionário de dados de saída
        caso as mesmas existam
        """
        if self.tem_dados_saida():
            self._dados_saida = {
                arq: pd.read_parquet(
                    self.caminho_saida / f"{arq}/ANO={self.ano}/REGIAO={self.reg}"
                ).assign(ANO=self.ano, REGIAO=self.reg)
                for arq in self.bases_saida
            }

    def tem_dados_saida(self) -> bool:
        """
        Verifica se o objeto ETL possuí todos os dados que fazem
        parte da sua saída

        :return: True se os dados estiver disponíveis
        """
        saidas = set(os.listdir(self.caminho_saida))
        if saidas.issuperset(set(self.bases_saida)):
            for b in self.bases_saida:
                sub = os.listdir(self.caminho_saida / b)
                if f"ANO={self.ano}" not in sub:
                    return False
                elif f"REGIAO={self.reg}" not in os.listdir(
                    self.caminho_saida / f"{b}/ANO={self.ano}"
                ):
                    return False
                elif f"{self.ano}.parquet" not in os.listdir(
                    self.caminho_saida / f"{b}/ANO={self.ano}/REGIAO={self.reg}"
                ):
                    return False
            return True
        else:
            return False

    def processa_tp(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: base de dados a ser processada
        """
        if "TP_ZONA_RESIDENCIAL" in base:
            if base["TP_ZONA_RESIDENCIAL"].min() == 0:
                base["TP_ZONA_RESIDENCIAL"] += 1

        super(_MatriculaRegiaoETL, self).processa_tp(base)

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        for arq, df in self.dados_saida.items():
            (self.caminho_saida / f"{arq}/ANO={self.ano}/REGIAO={self.reg}").mkdir(
                parents=True, exist_ok=True
            )

            df.drop(columns="ANO").to_parquet(
                self.caminho_saida
                / f"{arq}/ANO={self.ano}/REGIAO={self.reg}/{self.ano}.parquet",
                index=False,
            )


class MatriculaETL(_BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de matrícula do
    censo escolar
    """

    _etls: typing.List[_MatriculaRegiaoETL]

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL de dados de Matrícula

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            tabela="matricula",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )
        self._etls = [
            _MatriculaRegiaoETL(
                entrada=entrada,
                saida=saida,
                regiao=reg,
                ano=self.ano,
                criar_caminho=criar_caminho,
                reprocessar=reprocessar,
            )
            for reg in ["CO", "NORDESTE", "NORTE", "SUDESTE", "SUL"]
        ]

    @property
    def bases_saida(self) -> typing.List[str]:
        return ["aluno.parquet", "matricula.parquet"]

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        self._logger.info(
            "Os dados serão carregados de forma individual para controlar o uso de memória"
        )
        self._dados_entrada = dict()

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saída de interesse

        Para este objeto nós fazemos o processamento dos dados de cada região
        por meio de ETLs para cada uma
        """
        for etl in self._etls:
            self._logger.info(f"----- PROCESSANDO DADOS PARA REGIÃO {etl.reg} -----")
            etl.extract()
            etl.transform()

    def load(self) -> None:
        """
        Exporta os dados transformados utilizando o _MatriculaRegiaoETL
        """
        for etl in self._etls:
            etl.load()

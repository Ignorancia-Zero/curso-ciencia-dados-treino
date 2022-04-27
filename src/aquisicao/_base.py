import abc
import os
import typing
import logging
from pathlib import Path

import pandas as pd


class _BaseETL(abc.ABC):
    """
    Esta classe representa o esqueleto padrão de um objeto ETL

    Neste objeto deverão ser informados os caminhos para as pastas
    de entrada e de saída de dados, além de uma flag informando se
    nós devemos re-processar os dados.

    Para executar o ETL, o usuário geralmente chamará o método
    pipeline, na qual executaremos sequencialmente o extract, transform
    e load.

    Para o extract, será checado se as bases_entrada estão disponíveis.
    Se for o caso, e a flag reprocessar estiver desativada, nós não iremos
    fazer o download dos dados, mas apenas a extração do mesmo

    Para o transform, será checado se as bases_saida estão disponíveis.
    Se for o caso, e a flag reprocessar estiver desativada, nós iremos carregar
    os dados do disco no dicionário de bases de saída, caso contrário nós
    vamos executar o método _transform

    Para o load, será checado se as bases_saida estão disponíveis.
    Se for o caso, e a flag reprocessar estiver desativada, nós não iremos fazer
    nada, caso contrário nós iremos exportar as bases como parquet

    Por fim, para o pipeline nós iremos verificar se existe alguma necessidade
    de reprocessamento dos dados, seja porque os dados de entrada ou saída estão
    indisponíveis, ou porque a flag reprocessar está ativada. Se houver necessidade
    de gerar os dados, então as etapas do processo serão executadas

    Por definição cada objeto irá definir o conjunto de métodos:
    - bases_entrada: Lista as bases que fazem parte da entrada do objeto
    - bases_saida: Lista as bases que fazem parte da saída do objeto
    - _download: Realiza o download dos dados para a máquina
    - _extract: Carrega as bases na memória
    - _transform: Transforma os dados de entrada nas bases de saída
    """

    caminho_entrada: Path
    caminho_saida: Path
    reprocessar: bool
    _dados_entrada: typing.Union[None, typing.Dict[str, pd.DataFrame]]
    _dados_saida: typing.Union[None, typing.Dict[str, pd.DataFrame]]
    _logger: logging.Logger

    def __init__(
        self,
        entrada: str,
        saida: str,
        criar_caminho: bool = True,
        reprocessar: bool = True,
    ) -> None:
        """
        Instância o objeto de ETL Base

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        self.caminho_entrada = Path(entrada)
        self.caminho_saida = Path(saida)
        self.reprocessar = reprocessar

        if criar_caminho:
            self.caminho_entrada.mkdir(parents=True, exist_ok=True)
            self.caminho_saida.mkdir(parents=True, exist_ok=True)

        self._dados_entrada = None
        self._dados_saida = None

        self._logger = logging.getLogger(__name__)

    def __str__(self) -> str:
        """
        Representação de texto da classe
        """
        return self.__class__.__name__

    @property
    def dados_entrada(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário de dados de entrada
        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if self._dados_entrada is None:
            self.extract()
        return self._dados_entrada

    @abc.abstractmethod
    @property
    def bases_entrada(self) -> str:
        """
        Lista o nome dos arquivos de entrada

        :return: lista de arquivos que compõem as bases de entrada
        """
        raise NotImplementedError

    @abc.abstractmethod
    @property
    def bases_saida(self) -> str:
        """
        Lista o nome dos arquivos de saída

        :return: lista de arquivos que compõem as bases de saída
        """
        raise NotImplementedError

    @property
    def precisa_reprocessar(self) -> bool:
        """
        Indica se há necessidade de reprocessar os dados

        :return: True se algum dado está faltando
        """
        dados = set(os.listdir(self.caminho_entrada))
        saidas = set(os.listdir(self.caminho_saida))
        return (
            not dados.issuperset(set(self.bases_entrada))
            or not saidas.issuperset(set(self.bases_saida))
            or self.reprocessar
        )

    @property
    def dados_saida(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário de dados de saída
        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if self._dados_saida is None:
            self.transform()
        return self._dados_saida

    @abc.abstractmethod
    def _download(self) -> None:
        """
        Realiza o download das bases de dados que serão utilizadas pelo objeto
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _extract(self) -> None:
        """
        Carrega as bases de dados que foram baixadas na memória pelo pandas
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        raise NotImplementedError

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        self._logger.info(f"EXTRAINDO DADOS DO OBJETO > {self}")
        dados = set(os.listdir(self.caminho_entrada))
        if not dados.issuperset(set(self.bases_entrada)) or self.reprocessar:
            self._download()
        self._extract()

    def transform(self) -> None:
        """
        Extraí os dados do objeto
        """
        self._logger.info(f"TRANSFORMANDO DADOS DO OBJETO > {self}")
        saidas = set(os.listdir(self.caminho_saida))
        if not saidas.issuperset(set(self.bases_saida)) or self.reprocessar:
            self._transform()
        else:
            self._dados_saida = {
                arq: pd.read_parquet(self.caminho_saida / arq)
                for arq in self.bases_saida
            }

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        self._logger.info(f"CARREGANDO DADOS DO OBJETO > {self}")
        saidas = set(os.listdir(self.caminho_saida))
        if not saidas.issuperset(set(self.bases_saida)) or self.reprocessar:
            for arq, df in self.dados_saida.items():
                df.to_parquet(self.caminho_saida / arq, index=False)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados
        """
        if self.precisa_reprocessar:
            self.extract()
            self.transform()
            self.load()

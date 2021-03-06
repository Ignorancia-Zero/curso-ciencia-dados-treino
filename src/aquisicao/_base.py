import abc
import logging
import os
import typing
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
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]
    _logger: logging.Logger

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
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

        self._dados_entrada = dict()
        self._dados_saida = dict()

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
        if len(self._dados_entrada) < len(self.bases_entrada):
            self.extract()
        return self._dados_entrada

    @property
    @abc.abstractmethod
    def bases_entrada(self) -> typing.List[str]:
        """
        Lista o nome dos arquivos de entrada

        :return: lista de arquivos que compõem as bases de entrada
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def bases_saida(self) -> typing.List[str]:
        """
        Lista o nome dos arquivos de saída

        :return: lista de arquivos que compõem as bases de saída
        """
        raise NotImplementedError

    def tem_dados_entrada(self) -> bool:
        """
        Verifica se o objeto ETL possuí todos os dados que fazem
        parte da sua entrada

        :return: True se os dados estiver disponíveis
        """
        tem_dados = True
        for b in self.bases_entrada:
            tem_dados = tem_dados and os.path.exists(self.caminho_entrada / b)
        return tem_dados

    def tem_dados_saida(self) -> bool:
        """
        Verifica se o objeto ETL possuí todos os dados que fazem
        parte da sua saída

        :return: True se os dados estiver disponíveis
        """
        tem_dados = True
        for b in self.bases_saida:
            tem_dados = tem_dados and os.path.exists(self.caminho_saida / b)
        return tem_dados

    def carrega_saidas(self) -> None:
        """
        Carrega os dados de saída no dicionário de dados de saída
        caso as mesmas existam
        """
        if self.tem_dados_saida():
            self._dados_saida = {
                arq: pd.read_parquet(self.caminho_saida / arq)
                for arq in self.bases_saida
            }

    @property
    def precisa_reprocessar(self) -> bool:
        """
        Indica se há necessidade de reprocessar os dados

        :return: True se algum dado está faltando
        """
        return (
            not self.tem_dados_entrada()
            or not self.tem_dados_saida()
            or self.reprocessar
        )

    @property
    def dados_saida(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário de dados de saída

        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if len(self._dados_saida) < len(self.bases_saida):
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

    def _load(self) -> None:
        """
        Método load protegido que carrega as bases de saída
        """
        for arq, df in self.dados_saida.items():
            df.to_parquet(self.caminho_saida / f"{arq}.parquet", index=False)

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        self._logger.info(f"EXTRAINDO DADOS DO OBJETO > {self}")
        if not self.tem_dados_entrada() or self.reprocessar:
            self._download()
        self._extract()

    def transform(self) -> None:
        """
        Extraí os dados do objeto
        """
        self._logger.info(f"TRANSFORMANDO DADOS DO OBJETO > {self}")
        if not self.tem_dados_saida() or self.reprocessar:
            self._transform()
        else:
            self.carrega_saidas()

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        self._logger.info(f"CARREGANDO DADOS DO OBJETO > {self}")
        if not self.tem_dados_saida() or self.reprocessar:
            self._load()

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados
        """
        if self.precisa_reprocessar:
            self.extract()
            self.transform()
            self.load()

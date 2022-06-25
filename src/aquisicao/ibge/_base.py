import abc
import os
import re
import typing
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.aquisicao._base import _BaseETL
from src.utils import obtem_extensao
from src.utils.info import carrega_csv
from src.utils.web import download_dados_web
from src.utils.web import obtem_pagina


def extrai_link(url: str) -> typing.List[str]:
    """
    Obtém todos os links de uma página do FTP do IBGE

    :param url: página do FTP para processar
    :return: links para dados na página
    """
    # carrega a página HTML
    soup = obtem_pagina(url)

    # encontra todos os elementos da página
    return [
        td.find("a").attrs["href"]
        for td in soup.find_all("td")
        if td.find("a")
        if td.find("a").contents != ["Parent Directory"]
    ]


def lista_arquivos_ftp(url: str) -> typing.Dict[str, str]:
    """
    Lista todos os arquivos contidos em um link para um FTP do IBGE

    :param url: caminho para página no FTP
    :return: dicionário com nome do arquivo e link para download
    """
    if url[-1] != "/":
        url = f"{url}/"

    # carrega a lista de formatos
    formatos = carrega_csv("formato_arquivos.csv")["Formato"].values

    # percorre todos os potenciais arquivos
    arquivos = dict()
    for link in extrai_link(url):
        # adiciona o arquivo a lista
        ext = obtem_extensao(link)
        if ext in formatos:
            arquivos[link] = f"{url}{link}"

        # se for um diretório chama essa função novamente
        # com o link atualizado
        else:
            arquivos.update(lista_arquivos_ftp(f"{url}{link}"))

    return arquivos


class _BaseFTPIBGE(_BaseETL, abc.ABC):
    """
    Classe que funciona como estrutura básica para carregamento
    de dados de algum dos FTPs do IBGE
    """

    # URL base para os FTP do IBGE
    URL_GEO: str = "https://geoftp.ibge.gov.br"
    URL: str = "https://ftp.ibge.gov.br"

    _base: str  # expressão regular usada para pesquisar por nome de arquivo no ibge
    _url: str  # URL adicional para um dos caminhos do IBGE (pasta do FTP)
    _geo: bool  # flag se estamos usando uma base do geoftp
    _ibge: typing.Dict[str, str]  # dicionário de links por base de dados

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        sub_pasta: str = "",
        url: str = "",
        geo: bool = False,
        base: str = "(.*?)",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL IBGE

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param sub_pasta: sub-pasta do IBGE para ser usada com entrada e saída
        :param url: pasta do FTP para acessar
        :param geo: flag se estamos usando uma base do geoftp
        :param base: Nome da base que vai na URL do INEP
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        # ajusta os caminhos de entrada e saída
        entrada = Path(entrada) / "ibge" / sub_pasta

        # chama o construtor do ETL Base
        super().__init__(entrada, saida, criar_caminho, reprocessar)

        # guarda informação da base a ser baixada
        self._base = base

        # constrói a URL para a pasta de arquivos
        self._geo = geo
        if geo:
            self._url = f"{self.URL_GEO}/{url}"
        else:
            self._url = f"{self.URL}/{url}"

    @property
    def ibge(self) -> typing.Dict[str, str]:
        """
        Realiza o web-scraping da página de dados do IBGE e popula
        um dicionário com o nome de cada base disponível e o link
        para baixar os dados

        :return: dicionário com nome do arquivo e link para a página
        """
        if not hasattr(self, "_ibge"):
            # lista arquivos do FTP
            arqs = lista_arquivos_ftp(self._url)

            # constrói o dicionário
            self._ibge = {
                arq: link for arq, link in arqs.items() if re.match(self._base, arq)
            }
        return self._ibge

    @property
    def bases_entrada(self) -> typing.List[str]:
        """
        Lista o nome dos arquivos de entrada

        :return: lista de arquivos que compõem as bases de entrada
        """
        return [b for b in self.ibge]

    def carrega_saidas(self) -> None:
        """
        Carrega os dados de saída no dicionário de dados de saída
        caso as mesmas existam
        """
        if self.tem_dados_saida():
            if not self._geo:
                self._dados_saida = {
                    arq: pd.read_parquet(self.caminho_saida / arq)
                    for arq in self.bases_saida
                }
            else:
                self._dados_saida = {
                    arq: gpd.read_parquet(self.caminho_saida / arq)
                    for arq in self.bases_saida
                }

    def _download(self) -> None:
        """
        Realiza o download das bases de dados que serão utilizadas pelo objeto
        """
        for base, link in self.ibge.items():
            if base not in os.listdir(self.caminho_entrada) or self.reprocessar:
                download_dados_web(self.caminho_entrada / base, link)

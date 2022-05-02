import os
import re
import typing
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.aquisicao._base import _BaseETL
from src.utils.web import download_dados_web
from src.utils.web import obtem_pagina


class IDEBETL(_BaseETL):
    """
    Faz o processamento de dados do IDEB
    """

    URL: str = "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados"
    _links: typing.Dict[str, str]

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL IDEB

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        super().__init__(entrada, saida, criar_caminho, reprocessar)

        # substitui os valores de entrada e saída
        self.caminho_entrada = self.caminho_entrada / "ideb"
        if criar_caminho:
            self.caminho_entrada.mkdir(parents=True, exist_ok=True)

    @property
    def links(self) -> typing.Dict[str, str]:
        """
        Realiza o web-scraping da página de dados do INEP

        :return: dicionário com nome do arquivo e link para a página
        """
        if not hasattr(self, "_links"):
            # lê a página web
            soup = obtem_pagina(self.URL)

            # extraí a lista de links
            self._links = {
                a["href"].split("/")[-1]: a["href"]
                for a in soup.find_all("a", attrs={"class": "external-link"})
                if "escola" in a.attrs["href"] and "download" in a.attrs["href"]
            }
        return self._links

    @property
    def bases_entrada(self) -> typing.List[str]:
        """
        Lista o nome dos arquivos de entrada

        :return: lista de arquivos que compõem as bases de entrada
        """
        return list(self.links)

    @property
    def bases_saida(self) -> typing.List[str]:
        return ["ideb"]

    def _download(self) -> None:
        """
        Realiza o download das bases de dados que serão utilizadas pelo objeto
        """
        for base, link in self.links.items():
            if base not in os.listdir(self.caminho_entrada) or self.reprocessar:
                download_dados_web(self.caminho_entrada / base, link)

    def _extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        # inicializa os dados de entrada como um dicionário vazio
        self._dados_entrada = dict()

        # para cada arquivo do censo demográfico
        for base in tqdm(self.bases_entrada):
            # para cada arquivo do censo
            with zipfile.ZipFile(self.caminho_entrada / base) as z:
                # carrega o arquivo de excel
                padrao_comp = f"({os.path.splitext(base)[0]})[.](xlsx|XLSX|xls|XLS)"
                arq = [
                    f
                    for f in z.namelist()
                    if re.search(padrao_comp, f.lower()) is not None
                ][0]
                self._dados_entrada[base] = pd.read_excel(z.open(arq), skiprows=9)

    @staticmethod
    def extrai_turma(base: str) -> str:
        """
        Extraí o ano do IDEB sendo processado como uma sigla
        AI = Anos iniciais
        AF = Anos finais
        EM = Ensino Médio

        :param base: noma da base a ser processado
        :return: string com sigla dos anos
        """
        if "anos_finais" in base:
            return "AF"
        elif "anos_iniciais" in base:
            return "AI"
        else:
            return "EM"

    @staticmethod
    def seleciona_dados(df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajusta os dados do documento para extrair um data frame
        com apenas as colunas e linhas de interesse

        :param df: dados de IDEB
        :return: data frame com dados de interesse
        """
        return (
            df.iloc[:-3, :]
            .drop(
                columns=[
                    "SG_UF",
                    "CO_MUNICIPIO",
                    "NO_MUNICIPIO",
                    "NO_ESCOLA",
                    "REDE",
                ]
            )
            .assign(ID_ESCOLA=lambda f: f["ID_ESCOLA"].astype("uint32"))
        )

    @staticmethod
    def obtem_metricas(df: pd.DataFrame, turma: str) -> pd.DataFrame:
        """
        Processa o data frame com os dados do IDEB extraíndo o de-para
        entre o nome da coluna na tabela original, o nome da métrica e
        o ano dos dados

        :param df: dados de IDEB
        :param turma: anos sendo processado (AI, AF, EM)
        :return: data frame com de-para
        """
        proc: typing.Dict[str, typing.List[typing.Union[str, int]]] = dict(
            COLUNA=list(), METRICA=list(), ANO=list()
        )
        for c in df.columns[1:]:
            i = c.find("_20")

            # extraí o ano
            a = int(c[i + 1 : i + 5])

            # obtém a métrica registrada
            m = c[:i]

            # adiciona a informação da turma a métrica
            t = c.replace(f"{m}_{a}", "")
            if t.startswith("_"):
                if "SI" in t:
                    m = f"{m}_{turma}"
                else:
                    m = f"{m}_{turma}{t}"
            else:
                m = f"{m}_{turma}"

            # ajustado os dados do campo
            m = m.replace("VL_", "").replace("INDICADOR_", "")
            if m == f"OBSERVADO_{turma}":
                m = f"IDEB_{turma}"
            elif m == f"PROJECAO_{turma}":
                m = f"IDEB_META_{turma}"

            # adiciona a base
            proc["COLUNA"].append(c)
            proc["METRICA"].append(m)
            proc["ANO"].append(a)

        # transforma o de-para em um data frame
        return pd.DataFrame(proc)

    @staticmethod
    def formata_resultados(df: pd.DataFrame, dados: pd.DataFrame) -> pd.DataFrame:
        """
        Formata o dataframe para as saídas esperadas da base de IDEB
        :param df: data frame com os dados processados
        :param dados: de-para de coluna e métrica/ano
        :return: base de saída formatada
        """
        return (
            df.melt(id_vars="ID_ESCOLA", var_name="COLUNA", value_name="VALOR")
            .merge(dados)
            .assign(
                VALOR=lambda f: f["VALOR"]
                .astype(str)
                .str.replace("[*]", "")
                .str.replace("[,]", ".")
                .replace({"-": np.nan, "ND": np.nan})
                .astype("float32")
            )
            .pivot_table(
                index=["ID_ESCOLA", "ANO"],
                columns=["METRICA"],
                values="VALOR",
                aggfunc="first",
            )
            .reset_index()
        )

    @staticmethod
    def concatena_saidas(saidas: typing.List[pd.DataFrame]) -> pd.DataFrame:
        """
        Concatena as bases de dados de IDEB
        :param saidas: lista com bases de IDEB
        :return: base concatenada única
        """
        df = pd.DataFrame()
        for s in saidas:
            if df.shape[0] == 0:
                df = s
            else:
                df = df.merge(s, on=["ID_ESCOLA", "ANO"], how="outer")
        return df

    def _transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        saidas = list()
        for base, df in self.dados_entrada.items():
            self._logger.info(f"Processando dados {base}")

            # obtém o tipo de ano
            turma = self.extrai_turma(base)

            # remove as colunas não utilizadas e converte os dados de código de escola
            df = self.seleciona_dados(df)

            # extraí as métricas reportadas na base
            dados = self.obtem_metricas(df, turma)

            # realiza o melt dos dados e obtém os nomes de campo ajustados
            df = self.formata_resultados(df, dados)
            saidas.append(df)

        self._logger.info("Concatenando saídas")
        self._dados_saida[self.bases_saida[0]] = self.concatena_saidas(saidas)

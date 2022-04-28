import abc
import os
import typing
from pathlib import Path

from src.aquisicao._base import _BaseETL
from src.utils.web import download_dados_web
from src.utils.web import obtem_pagina


class _BaseINEPETL(_BaseETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do INEP
    """

    # URL base para todos os micro-dados do INEP
    # URL: str = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/"
    URL: str = "https://iz-ccdd.herokuapp.com/"

    _base: str  # lista de bases que devem ser baixadas
    _url: str  # URL completa a lista de micro-dados
    _inep: typing.Dict[str, str]  # dicionário de links por base de dados

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        base: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL INEP

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param base: Nome da base que vai na URL do INEP
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        super().__init__(entrada, saida, criar_caminho, reprocessar)

        self._sub_pasta = base.replace("-", "_").replace(" ", "_")
        self._base = base.replace("-", "_")
        self._ano = ano
        self._url = f"{self.URL}/{base}"

        # substitui os valores de entrada e saída
        self.caminho_entrada = self.caminho_entrada / f"{self._sub_pasta}"
        if criar_caminho:
            self.caminho_entrada.mkdir(parents=True, exist_ok=True)

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
                elif f"{self.ano}.parquet" not in os.listdir(
                    self.caminho_saida / f"{b}/ANO={self.ano}"
                ):
                    return False
            return True
        else:
            return False

    @property
    def inep(self) -> typing.Dict[str, str]:
        """
        Realiza o web-scraping da página de dados do INEP e popula
        um dicionário com o nome de cada base disponível e o link
        para baixar os dados

        :return: dicionário com nome do arquivo e link para a página
        """

        if not hasattr(self, "_inep"):
            soup = obtem_pagina(self._url)
            self._inep = {
                tag.text[::-1][:4][::-1] + ".zip": tag["href"]
                for tag in soup.find_all("a", {"class": "external-link"})
            }
        return self._inep

    @property
    def ano(self) -> int:
        """
        Ano da base sendo processado pelo objeto

        :return: ano como um número inteiro
        """
        if isinstance(self._ano, str):
            if self._ano == "ultimo":
                return max([int(b[:4]) for b in self.inep])
            else:
                if self._ano.isnumeric():
                    return int(self._ano)
                else:
                    raise ValueError(f"Não conseguimos processar ano={self._ano}")
        else:
            return self._ano

    @property
    def bases_entrada(self) -> typing.List[str]:
        """
        Lista o nome dos arquivos de entrada

        :return: lista de arquivos que compõem as bases de entrada
        """
        return [b for b in self.inep if int(b[:4]) == self.ano]

    def _download(self) -> None:
        """
        Realiza o download das bases de dados que serão utilizadas pelo objeto
        """
        base = f"{self.ano}.zip"
        link = self.inep[base]
        if base not in os.listdir(self.caminho_entrada) or self.reprocessar:
            download_dados_web(self.caminho_entrada / base, link)

    def _load(self) -> None:
        """
        Exporta os dados transformados
        """
        for arq, df in self.dados_saida.items():
            (self.caminho_saida / f"{arq}/ANO={self.ano}").mkdir(parents=True, exist_ok=True)

            df.drop(columns="ANO").to_parquet(
                self.caminho_saida / f"{arq}/ANO={self.ano}/{self.ano}.parquet",
                index=False,
            )

import abc
import os
import typing
from pathlib import Path

import geopandas as gpd

from ._base import _BaseFTPIBGE
from ._base import extrai_link


class _BaseMalhaIBGE(_BaseFTPIBGE, abc.ABC):
    """
    Classe que foi estruturada para realizar o download e processamento
    de dados de uma malha geográfica do IBGE
    """

    URL_BASE: typing.Dict[str, str] = {
        "mun": "organizacao_do_territorio/malhas_territoriais/malhas_municipais",
        "uf": "organizacao_do_territorio/malhas_territoriais/malhas_municipais",
        "meso": "organizacao_do_territorio/malhas_territoriais/malhas_municipais",
        "micro": "organizacao_do_territorio/malhas_territoriais/malhas_municipais",
        "brasil": "organizacao_do_territorio/malhas_territoriais/malhas_municipais",
    }

    GRANULARIDADE: typing.Dict[str, str] = {
        "mun": "municipio_{ano}/Brasil/BR",
        "uf": "municipio_{ano}/Brasil/BR",
        "meso": "municipio_{ano}/Brasil/BR",
        "micro": "municipio_{ano}/Brasil/BR",
        "brasil": "municipio_{ano}/Brasil/BR",
    }

    BASE: typing.Dict[str, str] = {
        "mun": "BR_Municipios_{ano}.zip",
        "uf": "BR_UF_{ano}.zip",
        "meso": "BR_Mesorregioes_{ano}.zip",
        "micro": "BR_Microrregioes_{ano}.zip",
        "brasil": "BR_Pais_{ano}.zip",
    }

    _ano: typing.Union[int, str]
    _granularidade: str

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        granularidade: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL INEP

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param granularidade: nível geográfico a ser processado
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        assert granularidade in self.GRANULARIDADE
        self._ano = ano
        self._granularidade = granularidade
        url = (
            self.URL_BASE[granularidade]
            + "/"
            + self.GRANULARIDADE[granularidade].format(ano=self.ano)
        )
        super().__init__(
            entrada=entrada,
            saida=saida,
            sub_pasta="malha",
            url=url,
            geo=True,
            base=self.BASE[granularidade].format(ano=self.ano),
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

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

    def carrega_saidas(self) -> None:
        """
        Carrega os dados de saída no dicionário de dados de saída
        caso as mesmas existam
        """
        if self.tem_dados_saida():
            self._dados_saida = {
                arq: gpd.read_parquet(
                    self.caminho_saida / f"{arq}/ANO={self.ano}"
                ).assign(ANO=self.ano)
                for arq in self.bases_saida
            }

    @property
    def ano(self) -> int:
        """
        Ano da base sendo processado pelo objeto

        :return: ano como um número inteiro
        """
        if isinstance(self._ano, str):
            if self._ano == "ultimo":
                anos = [
                    int(a.replace("municipio_", "").replace("/", ""))
                    for a in extrai_link(
                        self.URL_GEO + "/" + self.URL_BASE[self._granularidade]
                    )
                ]
                self._ano = sorted(anos)[-1]
            else:
                if self._ano.isnumeric():
                    self._ano = int(self._ano)
                else:
                    raise ValueError(f"Não conseguimos processar ano={self._ano}")
        return self._ano

    def _load(self) -> None:
        """
        Exporta os dados transformados
        """
        for arq, df in self.dados_saida.items():
            (self.caminho_saida / f"{arq}/ANO={self.ano}").mkdir(
                parents=True, exist_ok=True
            )

            df.drop(columns="ANO").to_parquet(
                self.caminho_saida / f"{arq}/ANO={self.ano}/{self.ano}.parquet",
                index=False,
            )

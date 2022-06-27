import typing
from pathlib import Path

import geopandas as gpd

from ._malha import _BaseMalhaIBGE


class MalhaBRIBGE(_BaseMalhaIBGE):
    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL INEP

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            granularidade="brasil",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

    @property
    def bases_saida(self) -> typing.List[str]:
        """
        Lista o nome dos arquivos de saída

        :return: lista de arquivos que compõem as bases de saída
        """
        return ["malha_br.parquet"]

    def _extract(self) -> None:
        """
        Carrega as bases de dados que foram baixadas na memória pelo pandas
        """
        self._dados_entrada[str(self.ano)] = gpd.read_file(
            "zip://" + str(self.caminho_entrada / self.bases_entrada[0])
        )

    def _transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saída de interesse
        """
        self._dados_saida[self.bases_saida[0]] = (
            self.dados_entrada[str(self.ano)]
            .rename(columns={"NM_PAIS": "NO_PAIS"})
            .assign(
                ANO=lambda f: self.ano,
                LATITUDE=lambda f: f["geometry"].apply(
                    lambda x: x.centroid.coords[0][1]
                ),
                LONGITUDE=lambda f: f["geometry"].apply(
                    lambda x: x.centroid.coords[0][0]
                ),
            )
        )

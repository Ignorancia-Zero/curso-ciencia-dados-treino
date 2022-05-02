import typing
from pathlib import Path

from src.aquisicao.inep._censo import _BaseCensoEscolarETL


class GestorETL(_BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de gestor do
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
        Instância o objeto de ETL de dados de Gestor

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            tabela="gestor",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

    @property
    def bases_saida(self) -> typing.List[str]:
        return ["gestor.parquet", "depara_gestor_escola.parquet"]

from pathlib import Path

from src.aquisicao.opcoes import ETL_DICT
from src.aquisicao.opcoes import EnumETL
from src.utils.logs import log_erros


@log_erros
def executa_etl(
    etl: str, entrada: Path, saida: Path, criar_caminho: bool, reprocessar: bool
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param entrada: string com caminho para pasta de entrada
    :param saida: string com caminho para pasta de saída
    :param criar_caminho: flag indicando se devemos criar os caminhos
    :param reprocessar: flag indicando se devemos reprocessar a base
    """
    objeto = ETL_DICT[EnumETL(etl)](entrada, saida, criar_caminho, reprocessar)
    objeto.pipeline()


@log_erros
def executa_etl_por_ano(
    etl: str,
    ano: str,
    entrada: Path,
    saida: Path,
    criar_caminho: bool,
    reprocessar: bool,
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param ano: ano do inep a ser processado
    :param entrada: string com caminho para pasta de entrada
    :param saida: string com caminho para pasta de saída
    :param criar_caminho: flag indicando se devemos criar os caminhos
    :param reprocessar: flag indicando se devemos reprocessar a base
    """
    objeto = ETL_DICT[EnumETL(etl)](entrada, saida, ano, criar_caminho, reprocessar)
    objeto.pipeline()

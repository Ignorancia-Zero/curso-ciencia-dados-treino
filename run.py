from pathlib import Path

import click

import src.configs as conf_geral
from src.aquisicao.executa import executa_etl
from src.aquisicao.opcoes import EnumETL
from src.utils.logs import configura_logs


@click.group()
def cli():
    pass


@cli.group()
def aquisicao():
    """
    Grupo de comandos que executam as funções de aquisição
    """
    pass


@aquisicao.command()
@click.option(
    "--etl",
    type=click.Choice([s.value for s in EnumETL]),
    help="Nome do ETL a ser executado",
)
@click.option(
    "--entrada",
    default=conf_geral.PASTA_ENTRADA_AQUISICAO,
    type=click.Path(file_okay=False, resolve_path=True, path_type=Path),
    help="Pasta para extração dos dados de aquisição",
)
@click.option(
    "--saida",
    default=conf_geral.PASTA_SAIDA_AQUISICAO,
    type=click.Path(file_okay=False, resolve_path=True, path_type=Path),
    help="Pasta para carregamento dos dados de aquisição",
)
@click.option(
    "--criar-caminho",
    is_flag=True,
    show_default=True,
    default=True,
    help="Flag indicando se devemos criar os caminhos",
)
@click.option(
    "--reprocessar",
    is_flag=True,
    show_default=True,
    default=True,
    help="Flag indicando se nós devemos forçar o reprocessamento dos dados",
)
def processa_dado(
    etl: str, entrada: str, saida: str, criar_caminho: bool, reprocessar: bool
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param entrada: string com caminho para pasta de entrada
    :param saida: string com caminho para pasta de saída
    :param criar_caminho: flag indicando se devemos criar os caminhos
    :param reprocessar: Flag indicando se nós devemos forçar o reprocessamento dos dados
    """
    configura_logs()
    executa_etl(
        etl=etl,
        entrada=entrada,
        saida=saida,
        criar_caminho=criar_caminho,
        reprocessar=reprocessar
    )


if __name__ == "__main__":
    cli()

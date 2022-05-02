from pathlib import Path

import click

import src.configs as conf_geral
from src.aquisicao.executa import executa_etl
from src.aquisicao.executa import executa_micro_inep
from src.aquisicao.opcoes import EnumETL
from src.aquisicao.opcoes import ETL_INEP_MICRO
from src.utils.logs import configura_logs
from src.datamart.executa import executa_datamart


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
    "--nao-criar-caminho",
    is_flag=True,
    show_default=True,
    help="Flag indicando se devemos criar os caminhos",
)
@click.option(
    "--nao-reprocessar",
    is_flag=True,
    show_default=True,
    help="Flag indicando se nós devemos forçar o reprocessamento dos dados",
)
def processa_dado(
    etl: str, entrada: str, saida: str, nao_criar_caminho: bool, nao_reprocessar: bool
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param entrada: string com caminho para pasta de entrada
    :param saida: string com caminho para pasta de saída
    :param nao_criar_caminho: flag indicando se devemos criar os caminhos
    :param nao_reprocessar: Flag indicando se nós devemos forçar o reprocessamento dos dados
    """
    configura_logs()
    executa_etl(
        etl=etl,
        entrada=entrada,
        saida=saida,
        criar_caminho=not nao_criar_caminho,
        reprocessar=not nao_reprocessar
    )


@aquisicao.command()
@click.option(
    "--etl",
    type=click.Choice([s.value for s in ETL_INEP_MICRO]),
    help="Nome do ETL a ser executado",
)
@click.option(
    "--ano",
    default="ultimo",
    help="Ano da base a ser processado",
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
    "--nao-criar-caminho",
    is_flag=True,
    show_default=True,
    help="Flag indicando se devemos criar os caminhos",
)
@click.option(
    "--nao-reprocessar",
    is_flag=True,
    show_default=True,
    help="Flag indicando se nós devemos forçar o reprocessamento dos dados",
)
def processa_micro_inep(
    etl: str, ano: str, entrada: str, saida: str, nao_criar_caminho: bool, nao_reprocessar: bool
) -> None:
    """
    Executa o pipeline de ETL de um micro-dado do INEP para um ano determinado

    :param etl: nome do ETL a ser executado
    :param ano: ano da base a ser processada
    :param entrada: string com caminho para pasta de entrada
    :param saida: string com caminho para pasta de saída
    :param nao_criar_caminho: flag indicando se devemos criar os caminhos
    :param nao_reprocessar: Flag indicando se nós devemos forçar o reprocessamento dos dados
    """
    configura_logs()
    executa_micro_inep(
        etl=etl,
        ano=ano,
        entrada=entrada,
        saida=saida,
        criar_caminho=not nao_criar_caminho,
        reprocessar=not nao_reprocessar
    )


@cli.group()
def datamart():
    """
    Grupo de comandos que executam as funções de datamart
    """
    pass


@datamart.command()
@click.option(
    "--granularidade",
    type=click.Choice([s.value for s in DMGran]),
    help="Nome do ETL a ser executado",
)
@click.option(
    "--ano",
    type=click.STRING,
    default="ultimo",
    help="Ano dos dados a serem processados (pode ser int ou 'ultimo')",
)
@click.option(
    "--aquisicao",
    default=conf_geral.PASTA_SAIDA_AQUISICAO,
    type=click.Path(file_okay=False, resolve_path=True, path_type=Path),
    help="Pasta para extração dos dados de aquisição",
)
@click.option(
    "--saida",
    default=conf_geral.PASTA_SAIDA_DATAMART,
    type=click.Path(file_okay=False, resolve_path=True, path_type=Path),
    help="Pasta para carregamento dos dados de aquisição",
)
def processa_datamart(granularidade: str, ano: str, aquisicao: Path, saida: Path) -> None:
    """
    Constrói um datamart a um determinado nível de granularidade para um
    dado ano de dados

    :param granularidade: nível do datamart a ser gerado
    :param ano: Ano dos dados a serem processados (pode ser int ou 'ultimo')
    :param aquisicao: caminho para pasta de dados processados na aquisição
    :param saida: caminho para pasta de saída
    """
    configura_logs()
    executa_datamart(granularidade, ano, aquisicao, saida)


if __name__ == "__main__":
    cli()

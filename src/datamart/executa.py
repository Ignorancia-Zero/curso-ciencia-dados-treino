import os
from pathlib import Path

from src.datamart.config import DMGran
from src.datamart.escola import controi_datamart_escola
from src.utils.logs import log_erros


@log_erros
def executa_datamart(
    granularidade: str, ano: str, aquis_entrada: Path, aquis_saida: Path, saida: Path
) -> None:
    """
    Constrói um datamart a um determinado nível de granularidade para um
    dado ano de dados

    :param granularidade: nível do datamart a ser gerado
    :param ano: ano da pesquisa a ser processado
    :param aquis_entrada: caminho para entrada de aquisição
    :param aquis_saida: caminho para saída de aquisição
    :param saida: caminho para pasta de saída
    """
    # obtém a granularidade
    gran = DMGran(granularidade)

    # obtém o ano
    if ano == "ultimo":
        ano = os.listdir("dados/externo/censo_escolar")[-1].split(".")[0]
    ano_int = int(ano)

    if gran == DMGran.ESCOLA:
        controi_datamart_escola(ano_int, aquis_entrada, aquis_saida, saida)
    else:
        raise NotImplementedError(
            f"Nós ainda temos que desenvolver o datamart para {granularidade}"
        )

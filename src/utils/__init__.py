import os
import typing


def obtem_extensao(arquivo: str) -> typing.Union[None, str]:
    """
    Obtém a extensão de um arquivo

    :param arquivo: nome do arquivo
    :return: string com a extensão
    """
    split = os.path.splitext(arquivo)
    if len(split) > 1:
        return split[-1][1:].lower()
    else:
        return None

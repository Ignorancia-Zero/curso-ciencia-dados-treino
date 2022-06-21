import typing
import urllib.request
from pathlib import Path

import bs4
import requests
from tqdm import tqdm


def obtem_pagina(url: str) -> bs4.BeautifulSoup:
    """
    Lê uma página Web utilizando a biblioteca requests

    :param url: url para processar
    :return: objeto BeautifulSoup com resultado da página
    """
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    res = urllib.request.urlopen(req).read()
    return bs4.BeautifulSoup(res, features="html.parser")


def download_dados_web(
    caminho: typing.Union[str, Path, typing.IO[bytes], typing.BinaryIO],
    url: str,
    block_size: int = 300 * 1024,
) -> typing.Union[typing.IO[bytes], typing.BinaryIO]:
    """
    Realiza o download dos dados em um link da Web

    :param caminho: caminho para extração dos dados
    :param url: endereço do site a ser baixado
    :param block_size: bloco em bytes para processar o arquivo
    :return: objeto buffer para o arquivo
    """
    # garante que o caminho é um buffer para um arquivo local
    fechar = isinstance(caminho, str) or isinstance(caminho, Path)
    if fechar:
        arq = open(caminho, "wb")  # type: ignore
    else:
        arq = caminho  # type: ignore

    # gera um request para os dados
    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get("content-length", 0))

    # processa a base
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)  # type: ignore
    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        arq.write(data)

    # fecha o buffer
    if fechar:
        arq.close()

    # retorna o buffer
    return arq

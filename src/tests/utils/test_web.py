import os
from io import BytesIO
from pathlib import Path

import bs4

from src.utils.web import download_dados_web
from src.utils.web import obtem_pagina


def test_obtem_pagina():
    pg = obtem_pagina("https://www.google.com/")
    assert isinstance(pg, bs4.BeautifulSoup)

    assert pg.find("title").text == "Google"


def test_download_dados_web(test_path: Path):
    buf = download_dados_web(
        caminho=test_path / "teste.txt",
        url="https://raw.githubusercontent.com/Ignorancia-Zero/curso-ciencia-dados-treino/main/requirements.txt",
    )

    assert os.path.exists(test_path / "teste.txt")
    assert buf.closed
    with open(test_path / "teste.txt") as f:
        data = f.read()
        assert "beautifulsoup4" in data

    buf = download_dados_web(
        caminho=BytesIO(),
        url="https://raw.githubusercontent.com/Ignorancia-Zero/curso-ciencia-dados-treino/main/requirements.txt",
    )
    assert not buf.closed

    buf.seek(0)
    data = buf.read().decode("utf-8")
    assert "beautifulsoup4" in data

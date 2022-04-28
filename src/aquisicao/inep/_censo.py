import abc
import re
import typing
import zipfile
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import rarfile

from src.aquisicao.inep._micro import _BaseINEPETL
from src.utils.info import carrega_excel
from src.utils.info import carrega_yaml


class _BaseCensoEscolarETL(_BaseINEPETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do CensoEscolar
    """

    _ano: typing.Union[str, int]
    _tabela: str
    _configs: typing.Dict[str, typing.Any]
    _regioes: str
    _carrega_cols: typing.List[str]
    _dtype: typing.Dict[str, str]
    _rename: typing.Dict[str, str]
    _cols_in: typing.List[str]

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        tabela: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
        regioes: typing.Sequence[str] = ("CO", "NORDESTE", "NORTE", "SUDESTE", "SUL"),
    ) -> None:
        """
        Instância o objeto de ETL Censo Escolar

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param tabela: Tabela do censo escolar a ser processada
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        :param regioes: lista de regiões que devem ser processadas
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            base="censo-escolar",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )
        self._tabela = tabela

        # carrega o arquivo YAML de configurações
        self._configs = carrega_yaml(f"aquis_censo_{tabela}.yml")

        # gera um regex de seleção de regiões
        self._regioes = "|".join([f"_{r.lower()}|_{r.upper()}" for r in regioes])

        # obtém a lista de colunas que devem ser extraídas da base
        ano = self.ano
        while ano > 2006:
            try:
                self._carrega_cols = (
                    carrega_excel(
                        f"aquis_censo_{tabela}_cols.xlsx", sheet_name=str(ano)
                    )
                    .loc[lambda f: f["USAR"] == 1, "COLUNA"]
                    .to_list()
                )
            except NameError:
                self._logger.warning(
                    f"Ano {ano} não encontrado na configuração de colunas, utilizando ano anteiror"
                )
                ano -= 1
            else:
                break

        # obtém o de-para de tipo e de nome
        dtype = carrega_excel(f"aquis_censo_{tabela}_cols.xlsx", sheet_name="dtype")
        self._dtype = dict(zip(dtype["COLUNA"].to_list(), dtype["DTYPE"].to_list()))
        self._rename = dict(zip(dtype["COLUNA"].to_list(), dtype["RENAME"].to_list()))

        # gera uma lista com todas as colunas do tipo IN_
        self._cols_in = (
            dtype.loc[lambda f: f["RENAME"].str.startswith("IN_"), "RENAME"]
            .drop_duplicates()
            .to_list()
        )

    @staticmethod
    def carrega_arquivo(
        arq: str, buffer: typing.Union[str, Path, typing.IO[bytes], BytesIO], **kwargs
    ) -> pd.DataFrame:
        """
        Le os dados de um arquivo contido em um buffer

        :param arq: arquivo a ser carregado
        :param buffer: buffer para o arquivo
        :return: data-frame
        """
        # e este arquivo for um CSV
        if ".csv" in arq.lower():
            return pd.read_csv(buffer, **kwargs)

        # caso seja outro arquivo zip
        elif ".zip" in arq.lower():
            with zipfile.ZipFile(buffer) as z:
                arq = z.namelist()[0]
                return pd.read_csv(z.open(arq), **kwargs)

        # caso seja um arquivo winrar
        elif ".rar" in arq.lower():
            with rarfile.RarFile(buffer) as z:
                arq = z.namelist()[0]
                return pd.read_csv(z.open(arq), **kwargs)

    def _extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        # para cada arquivo do censo
        with zipfile.ZipFile(self.caminho_entrada / f"{self.ano}.zip") as z:
            # gera o padrão de pesquisa
            padrao_comp = (
                f"({self._tabela.lower()}|{self._tabela.upper()}|{self._tabela.lower().title()})"
                f"({self._regioes})?"
                f"[.](csv|CSV|rar|RAR|zip|ZIP)"
            )
            conf = dict(encoding="latin-1", sep="|")

            # lista os conteúdos dos arquivos zip que contém o padrão
            arqs = [
                f for f in z.namelist() if re.search(padrao_comp, f.lower()) is not None
            ]

            # para cada arquivo a ser carregado
            for arq in arqs:
                # carrega uma versão dummy dos dados e compara contra os valores reais
                dummy = self.carrega_arquivo(arq, z.open(arq), nrows=10, **conf)
                total_cols = set(dummy.columns)
                if len(total_cols - set(self._dtype)) > 0:
                    self._logger.warning(
                        f"As colunas {total_cols - set(self._dtype)} foram adicionadas ao dataset, "
                        f"avalie se não é necessário adiciona-las ao arquivo de configuração"
                    )

            # adiciona as configurações de carregamento
            conf["usecols"] = self._carrega_cols
            conf["dtype"] = self._dtype

            # carrega os dados
            data = list()
            for arq in arqs:
                df = self.carrega_arquivo(arq, z.open(arq), **conf)
                if df is not None:
                    df.rename(columns=self._rename, inplace=True)
                    data.append(df)

            # verifica se algum dado foi carregado
            if len(data) == 0:
                raise ValueError(
                    f"As configurações do objeto não geraram qualquer base de dados"
                    f"de entrada -> {self._base} / {self._tabela} / {self._ano}"
                )

            # concatena as bases carregadas
            self._dados_entrada[str(self.ano)] = pd.concat(data)

    @staticmethod
    def obtem_operacao(
        op: str,
    ) -> typing.Callable[[pd.DataFrame, typing.List[str]], pd.Series]:
        """
        Recebe uma string com o tipo de operação de comparação a ser realiza e retorna uma
        função que receberá um dataframe e uma lista de colunas e devolverá uma série de booleanos
        comparando a soma desta lista de colunas por linha ao valor 0

        :param op: operação de comparação (=, >, <, >=, <=, !=)
        :return: função que compara soma de colunas ao valor 0
        """
        if op == "=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) == 0
        elif op == ">":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) > 0
        elif op == "<":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) < 0
        elif op == ">=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) >= 0
        elif op == "<=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) <= 0
        elif op == "!=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) != 0
        else:
            raise ValueError(
                f"O operador {op} não faz parte da lista de operações disponíveis"
            )

    @staticmethod
    def gera_coluna_por_comparacao(
        base: pd.DataFrame,
        colunas_a_tratar: typing.Dict[str, typing.List[str]],
    ) -> None:
        """
        Realiza a criação de novas colunas na base passada a partir de um outro conjunto de
        colunas que são somadas linha a linha e comparadas com o valor 0 por meio de algum operador

        :param base: base de dados a ser processada
        :param colunas_a_tratar: dicionário com configurações de tratamento
        """
        # percorre o dicionário de configurações
        for coluna, tratamento in colunas_a_tratar.items():
            # extraí o padrão de colunas de origem e a operação de comparação
            padrao, operacao = tratamento

            # obtém a lista de colunas de origem que devem ser utilizadas
            colunas_origem = [
                c for c in base.columns if re.search(padrao, c) is not None
            ]

            # se a coluna não existir na base e se nós temos colunas de origem
            if (coluna not in base.columns) and len(colunas_origem) > 0:
                # aplica a função de geração de colunas
                func = _BaseCensoEscolarETL.obtem_operacao(operacao)
                base[coluna] = func(base, colunas_origem).astype("int")

    def gera_dt_nascimento(self, base: pd.DataFrame) -> None:
        """
        Cria a coluna de data de nascimento

        :param base: base de dados a ser processada
        """
        if (
            "NU_ANO" in base
            and "NU_MES" in base
            and "DT_NASCIMENTO" in self._configs["DADOS_SCHEMA"]
        ):
            if "NU_DIA" in base:
                base["DT_NASCIMENTO"] = pd.to_datetime(
                    base["NU_ANO"].astype(str)
                    + base["NU_MES"].astype(str)
                    + base["NU_DIA"].astype(str),
                    format="%Y%m%d",
                )
            else:
                base["DT_NASCIMENTO"] = pd.to_datetime(
                    base["NU_ANO"].astype(str) + base["NU_MES"].astype(str),
                    format="%Y%m",
                )

    def processa_dt(self, base: pd.DataFrame) -> None:
        """
        Realiza a conversão das colunas de datas de texto para datetime

        :param base: base de dados a ser processada
        """
        colunas_data = [c for c in base.columns if c.startswith("DT_")]
        for c in colunas_data:
            try:
                base[c] = pd.to_datetime(base[c], format="%d/%m/%Y")
            except ValueError:
                base[c] = pd.to_datetime(base[c], format="%d%b%Y:00:00:00")

    def processa_qt(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas de quantidades

        :param base: base de dados a ser processada
        """
        if self.ano >= 2019:
            for c in self._configs["COLS_88888"]:
                if c in base:
                    base[c] = base[c].replace({88888: np.nan})

    def processa_in(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas indicadoras

        :param base: base de dados a ser processada
        """
        # gera a lista de todas as colunas existentes
        in_atual = [c for c in base if c.startswith("IN_")]
        cols = set(self._cols_in + in_atual)
        dif = set(in_atual) - set(self._cols_in)
        if len(dif) > 0:
            self._logger.warning(
                f"Há novas colunas IN que foram adicionadas -> {dif}"
                f"\nConsidere adiciona-las ao info/aquis_censo_{self._tabela}.yml"
            )

        # preenche bases com colunas IN quando há uma coluna QT
        for col in base:
            if (
                col[:2] == "QT"
                and f"IN{col[2:]}" in cols
                and f"IN{col[2:]}" not in base
            ):
                base[f"IN{col[2:]}"] = (base[col] > 0).astype("int")

        # realiza o tratamento das colunas IN_ a partir das configurações
        self.gera_coluna_por_comparacao(base, self._configs["TRATAMENTO_IN"])

    def processa_tp(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: base de dados a ser processada
        """
        # converte a coluna para tipo categórico
        for c, d in self._configs["DEPARA_TP"].items():
            if c in base:
                # lista os valores a serem categorizados
                vals = list(d.values())

                # obtém os valores
                unicos = set(base[c].dropna().replace(d))
                esperado = set(vals)

                # verifica que não há nenhum erro com os dados a serem preenchidos
                if not esperado.issuperset(unicos):
                    raise ValueError(
                        f"A coluna {c} da base possuí os valores "
                        f"{unicos - esperado} a mais"
                    )

                # cria o tipo categórico
                cat = pd.Categorical(vals).dtype

                # realiza a conversão da coluna
                base[c] = base[c].replace(d).astype(cat)

    def remove_duplicatas(self, base: pd.DataFrame) -> typing.Union[None, pd.DataFrame]:
        """
        Remove duplicatas na base devido a uma chave secundária que gera um de-para entre
        o nível de granularidade da base e alguma outra entidade do censo

        :param base: base de dados a ser processada
        :return: base de de-para
        """
        if len(self._configs["COLS_DEPARA"]) == 0:
            return None

        cols = [self._configs["COL_ID"], "ANO"] + self._configs["COLS_DEPARA"]
        base_id = base.reindex(columns=cols)

        self._logger.debug(base, base.shape)
        base.drop(columns=self._configs["COLS_DEPARA"], errors="ignore", inplace=True)
        base.drop_duplicates(inplace=True)

        self._logger.debug(base, base.shape)

        return base_id

    def ajusta_schema(
        self,
        base: pd.DataFrame,
        fill: typing.Dict[str, typing.Any],
        schema: typing.Dict[str, str],
    ) -> pd.DataFrame:
        """
        Modifica o schema de uma base para bater com as configurações

        :param base: base de dados a ser processada
        :param fill: dicionário de preenchimento por coluna
        :param schema: dicionário de tipo de dados por coluna
        """
        # garante que todas as colunas existam
        rm = set(base) - set(schema)
        ad = set(schema) - set(base)
        if len(rm) > 0:
            self._logger.warning(f"As colunas {rm} serão removidas do data set")
        if len(ad) > 0:
            self._logger.warning(f"As colunas {ad} serão adicionadas do data set")
        base = base.reindex(columns=schema)

        # preenche nulos com valores fixos
        for c, p in fill.items():
            if c in base:
                base[c] = base[c].fillna(p)

        # ajusta o schema
        for c, dtype in schema.items():
            if dtype.startswith("pd."):
                base[c] = base[c].astype(eval(dtype))
            elif dtype == "str":
                base[c] = base[c].astype(dtype).replace({"nan": None})
            else:
                base[c] = base[c].astype(dtype)

        return base

    def _transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saída de interesse
        """
        base = self.dados_entrada[str(self.ano)]

        self._logger.info("Gera DT nascimento")
        self.gera_dt_nascimento(base)

        self._logger.info("Processando colunas DT_")
        self.processa_dt(base)

        self._logger.info("Processando colunas QT_")
        self.processa_qt(base)

        self._logger.info("Processando colunas IN_")
        self.processa_in(base)

        self._logger.info("Processando colunas TP_")
        self.processa_tp(base)

        self._logger.info("Removendo informações duplicadas")
        base_id = self.remove_duplicatas(base)

        self._logger.info("Realizando ajustes finais na base")
        self._dados_saida[self.bases_saida[0]] = self.ajusta_schema(
            base=base,
            fill=self._configs["PREENCHER_NULOS"],
            schema=self._configs["DADOS_SCHEMA"],
        )
        if base_id:
            self._dados_saida[self.bases_saida[1]] = self.ajusta_schema(
                base=base_id,
                fill=self._configs["PREENCHER_NULOS"],
                schema=self._configs["DEPARA_SCHEMA"],
            )

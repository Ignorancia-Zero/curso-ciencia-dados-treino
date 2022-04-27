import re
import typing
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import rarfile

from src.aquisicao.inep._micro import _BaseINEPETL
from src.utils.info import carrega_yaml


class EscolaETL(_BaseINEPETL):
    """
    Classe que realiza o processamento de dados de escola
    do censo escolar
    """

    _configs: typing.Dict[str, typing.Any]

    def __init__(
        self,
        entrada: typing.Union[str, Path],
        saida: typing.Union[str, Path],
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL de dados de Escola

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag para forçar o re-processamento das bases de dados
        """
        super().__init__(
            entrada=entrada,
            saida=saida,
            base="censo-escolar",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

        self._configs = carrega_yaml("aquis_censo_escola.yml")

    @property
    def bases_saida(self) -> typing.List[str]:
        """
        Lista o nome dos arquivos de saída

        :return: lista de arquivos que compõem as bases de saída
        """
        return ["escola"]

    def _extract(self) -> None:
        """
        Carrega as bases de dados que foram baixadas na memória pelo pandas
        """
        # abre o arquivo zip com o conteúdo do censo
        with zipfile.ZipFile(self.caminho_entrada / f"{self._sub_pasta}/{self.ano}.zip") as z:
            # lista os conteúdos dos arquivos zip que contém o nome tabela
            arq = [f for f in z.namelist() if "escolas." in f.lower()][0]

            # e este arquivo for um CSV
            if ".csv" in arq.lower():
                # le os conteúdos do arquivo por meio do buffer do zip
                self._dados_entrada[str(self.ano)] = pd.read_csv(
                    z.open(arq), encoding="latin-1", sep="|"
                )

            # caso seja outro arquivo zip
            elif ".zip" in arq.lower():
                # cria um novo zipfile e le o arquivo deste novo zip
                with zipfile.ZipFile(z.open(arq)) as z2:
                    arq = z2.namelist()[0]
                    self._dados_entrada[str(self.ano)] = pd.read_csv(
                        z2.open(arq), encoding="latin-1", sep="|"
                    )

            # caso seja um arquivo winrar
            elif ".rar" in arq.lower():
                with rarfile.RarFile(z.open(arq)) as z2:
                    arq = z2.namelist()[0]
                    self._dados_entrada[str(self.ano)] = pd.read_csv(
                        z2.open(arq), encoding="latin-1", sep="|"
                    )

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
                func = EscolaETL.obtem_operacao(operacao)
                base[coluna] = func(base, colunas_origem).astype("int")

    def renomeia_colunas(self, base: pd.DataFrame) -> None:
        """
        Renomea as colunas da base de entrada

        :param base: base de escolas sendo tratada
        """
        base.rename(columns=self._configs["RENOMEIA_COLUNAS"], inplace=True)

    def dropa_colunas(self, base: pd.DataFrame) -> None:
        """
        Remove colunas que são redundantes com outras bases

        :param base: base de escolas sendo tratada
        """
        base.drop(
            columns=self._configs["DROPAR_COLUNAS"], inplace=True, errors="ignore"
        )

    def processa_dt(self, base: pd.DataFrame) -> None:
        """
        Realiza a conversão das colunas de datas de texto para datetime

        :param base: base de escolas sendo tratada
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

        :param base: base de escolas sendo tratada
        """
        if self.ano >= 2019:
            for c in self._configs["COLS_88888"]:
                if c in base:
                    base[c] = base[c].replace({88888: np.nan})

    def processa_in(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas indicadoras

        :param base: base de escolas sendo tratada
        """
        # gera a lista de todas as colunas indicadoras
        cols = set(self._configs["COLS_IN"])

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

        # cria a coluna IN_ENERGIA_OUTROS
        if "IN_ENERGIA_OUTROS" not in base and "IN_ENERGIA_INEXISTENTE" in base:
            base["IN_ENERGIA_OUTROS"] = (
                (
                    base[
                        [
                            c
                            for c in base
                            if "IN_ENERGIA" in c and c != "IN_ENERGIA_INEXISTENTE"
                        ]
                    ].sum(axis=1)
                    == 0
                )
                & (base["IN_ENERGIA_INEXISTENTE"] == 0)
            ).astype("int")

        # cria a coluna IN_LOCAL_FUNC_GALPAO
        if "IN_LOCAL_FUNC_GALPAO" not in base and "TP_OCUPACAO_GALPAO" in base:
            base["IN_LOCAL_FUNC_GALPAO"] = np.where(
                (base["TP_OCUPACAO_GALPAO"] > 0) & (base["TP_OCUPACAO_GALPAO"] <= 3),
                1,
                np.where(base["TP_OCUPACAO_GALPAO"] == 0, 0, np.nan),
            )

        # cria a coluna IN_LINGUA_INDIGENA e IN_LINGUA_PORTUGUESA
        if "IN_LINGUA_INDIGENA" not in base and "TP_INDIGENA_LINGUA" in base:
            base["IN_LINGUA_INDIGENA"] = (
                base["TP_INDIGENA_LINGUA"].isin([1, 3])
            ).astype("int")

        if "IN_LINGUA_PORTUGUESA" not in base and "TP_INDIGENA_LINGUA" in base:
            base["IN_LINGUA_PORTUGUESA"] = (
                base["TP_INDIGENA_LINGUA"].isin([2, 3])
            ).astype("int")

        # corrige a coluna IN_BIBLIOTECA
        if "IN_SALA_LEITURA" not in base and "IN_BIBLIOTECA" in base:
            if "IN_BIBLIOTECA_SALA_LEITURA" in base:
                base.drop(columns=["IN_BIBLIOTECA_SALA_LEITURA"], inplace=True)
            base.rename(
                columns={"IN_BIBLIOTECA": "IN_BIBLIOTECA_SALA_LEITURA"},
                inplace=True,
            )

        # cria a coluna IN_AGUA_POTAVEL
        if "IN_AGUA_POTAVEL" not in base and "IN_AGUA_FILTRADA" in base:
            base["IN_AGUA_POTAVEL"] = ((base["IN_AGUA_FILTRADA"] == 2)).astype("int")

        # substitui o valor 9 pelo valor nulo
        for c in base:
            if c.startswith("IN_"):
                base[c] = base[c].replace({9: np.nan})

    def processa_tp(self, base: pd.DataFrame) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: base de escolas sendo tratada
        """
        # cria a coluna TP_INDIGENA_LINGUA
        if (
            "TP_INDIGENA_LINGUA" not in base
            and "IN_LINGUA_INDIGENA" in base
            and "IN_LINGUA_PORTUGUESA" in base
        ):
            base["TP_INDIGENA_LINGUA"] = np.where(
                (base["IN_LINGUA_INDIGENA"] == 1) & (base["IN_LINGUA_PORTUGUESA"] == 0),
                1,
                np.where(
                    (base["IN_LINGUA_INDIGENA"] == 0)
                    & (base["IN_LINGUA_PORTUGUESA"] == 1),
                    2,
                    np.where(
                        (base["IN_LINGUA_INDIGENA"] == 1)
                        & (base["IN_LINGUA_PORTUGUESA"] == 1),
                        3,
                        np.where(
                            (
                                base["TP_SITUACAO_FUNCIONAMENTO"]
                                .astype("str")
                                .isin(["1", "1.0", "EM ATIVIDADE"])
                            )
                            & (base["IN_EDUCACAO_INDIGENA"] == 0),
                            0,
                            np.nan,
                        ),
                    ),
                ),
            )

        # Corrige o campo de TP_OCUPACAO_GALPAO
        if "TP_OCUPACAO_GALPAO" in base:
            if base["TP_OCUPACAO_GALPAO"].max() == 1:
                base.drop(columns=["TP_OCUPACAO_GALPAO"], inplace=True)

        # Corrige o campo de TP_OCUPACAO_PREDIO_ESCOLAR
        if "TP_OCUPACAO_PREDIO_ESCOLAR" in base:
            if base["TP_OCUPACAO_PREDIO_ESCOLAR"].max() == 1:
                base.drop(columns=["TP_OCUPACAO_PREDIO_ESCOLAR"], inplace=True)

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
                if c in self._configs["PREENCHER_TP"]:
                    vals += [self._configs["PREENCHER_TP"][c]]
                cat = pd.Categorical(vals).dtype

                # realiza a conversão da coluna
                base[c] = base[c].replace(d).astype(cat)

        # vamos converter as variáveis indicadores de escolas particulares
        # em variáveis TP, uma vez que teremos uma terceira opção informando
        # que a escola é pública
        for c in self._configs["COLS_PARTICULAR"]:
            if c in base:
                base.rename(columns={c: f"TP{c[2:]}"}, inplace=True)
                base[f"TP{c[2:]}"] = base[f"TP{c[2:]}"].replace({0: "NÃO", 1: "SIM"})
                base[f"TP{c[2:]}"] = np.where(
                    (base["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                    & (base["TP_DEPENDENCIA"] == "PRIVADA"),
                    base[f"TP{c[2:]}"],
                    np.where(
                        (base["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                        & ~(base["TP_DEPENDENCIA"] == "PRIVADA"),
                        base[f"TP{c[2:]}"].fillna("PÚBLICA"),
                        np.nan,
                    ),
                )
                base[f"TP{c[2:]}"] = base[f"TP{c[2:]}"].astype("category")

    def preenche_nulos(self, base: pd.DataFrame) -> None:
        """
        Realiza o preenchimento de valores nulos dentro da base temporal

        :param base: base de escolas sendo tratada
        """
        # faz o sorting e reset index
        base.sort_values(by=["CO_ENTIDADE", "ANO"], inplace=True)
        base.reset_index(drop=True, inplace=True)

        # remove colunas que são redundantes
        base.drop(columns=self._configs["REMOVER_COLS"], inplace=True, errors="ignore")

        # corrige variáveis de escolas privadas
        for c in self._configs["COLS_PARTICULAR"]:
            c = c if c.startswith("TP") else f"TP{c[2:]}"
            base[c] = np.where(
                (base[c] == "PÚBLICA") & (base["TP_DEPENDENCIA"] == "PRIVADA"),
                np.nan,
                base[c],
            )

        # corrige a ocupação de galpão
        base["TP_OCUPACAO_GALPAO"] = np.where(
            ~(base["TP_OCUPACAO_GALPAO"] == "NÃO")
            & ~(base["IN_LOCAL_FUNC_GALPAO"] == 0),
            np.nan,
            base["TP_OCUPACAO_GALPAO"],
        )

        # preenche nulos com valores fixos
        for c, p in self._configs["PREENCHER_NULOS"].items():
            base[c] = base[c].fillna(p)

    def _transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saída de interesse
        """
        base = self.dados_entrada[str(self.ano)]

        self._logger.info("Renomeando colunas das bases de escola")
        self.renomeia_colunas(base)

        self._logger.info("Dropando colunas redundantes com outras bases")
        self.dropa_colunas(base)

        self._logger.info("Processando colunas com datas")
        self.processa_dt(base)

        self._logger.info("Processando colunas QT_")
        self.processa_qt(base)

        self._logger.info("Processando colunas IN_")
        self.processa_in(base)

        self._logger.info("Processando colunas TP_")
        self.processa_tp(base)

        self._logger.info("Ajustando valores nulos")
        self.preenche_nulos(base)

        self._dados_saida["escola"] = base

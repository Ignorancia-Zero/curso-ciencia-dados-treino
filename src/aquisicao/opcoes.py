from enum import Enum

from src.aquisicao.inep.escola import EscolaETL


class EnumETL(Enum):
    escola = "ESCOLA"


# lista os objetos ETL que fazem parte dos micro-dados do inep
ETL_INEP_MICRO = [EnumETL.escola]


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT = {EnumETL.escola: EscolaETL}

from enum import Enum

from src.aquisicao.inep.escola import EscolaETL
from src.aquisicao.inep.gestor import GestorETL


class EnumETL(Enum):
    escola = "ESCOLA"
    gestor = "GESTOR"


# lista os objetos ETL que fazem parte dos micro-dados do inep
ETL_INEP_MICRO = [EnumETL.escola, EnumETL.gestor]


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT = {EnumETL.escola: EscolaETL, EnumETL.gestor: GestorETL}

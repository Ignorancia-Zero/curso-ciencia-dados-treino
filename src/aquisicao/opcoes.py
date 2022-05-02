from enum import Enum

from src.aquisicao.inep.escola import EscolaETL
from src.aquisicao.inep.gestor import GestorETL
from src.aquisicao.inep.turma import TurmaETL


class EnumETL(Enum):
    escola = "ESCOLA"
    gestor = "GESTOR"
    turma = "TURMA"


# lista os objetos ETL que fazem parte dos micro-dados do inep
ETL_INEP_MICRO = [EnumETL.escola, EnumETL.gestor, EnumETL.turma]


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT = {
    EnumETL.escola: EscolaETL,
    EnumETL.gestor: GestorETL,
    EnumETL.turma: TurmaETL,
}

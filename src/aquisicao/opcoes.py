from enum import Enum

from src.aquisicao.ibge.malha_br import MalhaBRIBGE
from src.aquisicao.ibge.malha_mun import MalhaMunIBGE
from src.aquisicao.ibge.malha_uf import MalhaUFIBGE
from src.aquisicao.inep.docente import DocenteETL
from src.aquisicao.inep.escola import EscolaETL
from src.aquisicao.inep.gestor import GestorETL
from src.aquisicao.inep.ideb import IDEBETL
from src.aquisicao.inep.matricula import MatriculaETL
from src.aquisicao.inep.turma import TurmaETL


class EnumETL(Enum):
    escola = "ESCOLA"
    gestor = "GESTOR"
    turma = "TURMA"
    docente = "DOCENTE"
    matricula = "MATRICULA"
    ideb = "IDEB"
    malha_mun = "MALHA_MUN"
    malha_uf = "MALHA_UF"
    malha_br = "MALHA_BR"


# lista os objetos ETL que fazem parte dos micro-dados do inep
ETL_ANUAL = [
    EnumETL.escola,
    EnumETL.gestor,
    EnumETL.turma,
    EnumETL.docente,
    EnumETL.matricula,
    EnumETL.malha_mun,
    EnumETL.malha_uf,
    EnumETL.malha_br,
]


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT = {
    EnumETL.escola: EscolaETL,
    EnumETL.gestor: GestorETL,
    EnumETL.turma: TurmaETL,
    EnumETL.docente: DocenteETL,
    EnumETL.matricula: MatriculaETL,
    EnumETL.ideb: IDEBETL,
    EnumETL.malha_mun: MalhaMunIBGE,
    EnumETL.malha_uf: MalhaUFIBGE,
    EnumETL.malha_br: MalhaBRIBGE,
}

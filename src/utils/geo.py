import typing

from pyproj import Geod
from shapely.geometry import Polygon, MultiPolygon


def calcula_area_poligono(poligono: typing.Union[Polygon, MultiPolygon]) -> float:
    """
    Calcula a área de um polígono em km quadrados

    :param poligono: objeto polígono a ter área calculada
    :return: área do polígono em km quadrados
    """
    geod = Geod(ellps="WGS84")
    return abs(geod.geometry_area_perimeter(poligono)[0]) / 1000000

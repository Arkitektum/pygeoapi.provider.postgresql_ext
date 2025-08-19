from typing import Dict, List, Any
from osgeo.osr import CoordinateTransformation, SpatialReference
from sqlalchemy.orm import Session
from cachetools import cached, TTLCache, keys
from pygeoapi.util import CrsTransformSpec

_sessions_cache = TTLCache(maxsize=640*1024, ttl=86400)


def get_coordinate_transformation(crs_transform_spec: CrsTransformSpec | None) -> CoordinateTransformation | None:
    if not crs_transform_spec:
        return None

    source: SpatialReference = SpatialReference()
    source.ImportFromWkt(crs_transform_spec.source_crs_wkt)

    target: SpatialReference = SpatialReference()
    target.ImportFromWkt(crs_transform_spec.target_crs_wkt)

    return CoordinateTransformation(source, target)


def get_target_epsg(crs_transform_spec: CrsTransformSpec | None, storage_crs: str) -> str:
    if crs_transform_spec:
        return _get_epsg(crs_transform_spec.target_crs_wkt)

    return _get_epsg_from_uri(storage_crs)


def add_geojson_crs(geojson: Dict, epsg: str) -> None:
    if epsg is None or epsg == 'CRS84':
        return

    geojson['crs'] = {
        'type': 'name',
        'properties': {
            'name': 'urn:ogc:def:crs:EPSG::' + epsg
        }
    }


@cached(cache=_sessions_cache, key=lambda table_model, id_field, session: keys.hashkey(table_model))
def get_table_ids(table_model, id_field, session: Session) -> List[Any]:
    id_column = getattr(table_model, id_field)
    result = session.query(id_column).order_by(id_column.asc())
    ids = [str(r[0]) for r in result]

    return ids


def clear_cache(table_model) -> None:
    key = keys.hashkey(table_model)
    _sessions_cache.pop(key)


def _get_epsg(wkt: str) -> str:
    sr: SpatialReference = SpatialReference()
    sr.ImportFromWkt(wkt)

    return sr.GetAuthorityCode(None)


def _get_epsg_from_uri(uri: str) -> str:
    return uri.split('/')[-1]


__all__ = ['get_coordinate_transformation', 'get_target_epsg',
           'add_geojson_crs', 'get_table_ids', 'clear_cache', ]

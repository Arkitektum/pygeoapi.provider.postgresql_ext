from typing import Dict
from osgeo.osr import CoordinateTransformation, SpatialReference
from pygeoapi.util import CrsTransformSpec


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


def _get_epsg(wkt: str) -> str:
    sr: SpatialReference = SpatialReference()
    sr.ImportFromWkt(wkt)

    return sr.GetAuthorityCode(None)


def _get_epsg_from_uri(uri: str) -> str:
    return uri.split('/')[-1]


__all__ = ['get_coordinate_transformation',
           'get_target_epsg', 'add_geojson_crs']

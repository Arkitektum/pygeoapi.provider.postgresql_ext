import json
from copy import deepcopy
import time
from typing import Dict, List, Any
from osgeo import ogr, osr
from sqlalchemy.orm import Session
from geoalchemy2 import WKBElement
from cachetools import cached, TTLCache, keys
from pygeoapi.provider.base import ProviderItemNotFoundError
from pygeoapi.provider.sql import PostgreSQLProvider
from pygeoapi.util import CrsTransformSpec

ogr.UseExceptions()
osr.UseExceptions()

_sessions_cache = TTLCache(maxsize=640*1024, ttl=86400)


class PostgreSQLExtendedProvider(PostgreSQLProvider):
    """
    A provider for querying a PostgreSQL database. With support for nonlinear geometry types
    """

    def __init__(self, provider_def: dict):
        super().__init__(provider_def)

    def query(
        self,
        offset=0,
        limit=10,
        resulttype='results',
        bbox=[],
        datetime_=None,
        properties=[],
        sortby=[],
        select_properties=[],
        skip_geometry=False,
        q=None,
        filterq=None,
        crs_transform_spec=None,
        **kwargs
    ):
        """
        Query sql database for all the content.
        e,g: http://localhost:5000/collections/hotosm_bdi_waterways/items?
        limit=1&resulttype=results

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)
        :param filterq: CQL query as text string
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """

        property_filters = self._get_property_filters(properties)
        cql_filters = self._get_cql_filters(filterq)
        bbox_filter = self._get_bbox_filter(bbox)
        time_filter = self._get_datetime_filter(datetime_)
        order_by_clauses = self._get_order_by_clauses(sortby, self.table_model)
        selected_properties = self._select_properties_clause(
            select_properties, skip_geometry
        )

        with Session(self._engine) as session:
            results = (
                session.query(self.table_model)
                .filter(property_filters)
                .filter(cql_filters)
                .filter(bbox_filter)
                .filter(time_filter)
                .options(selected_properties)
            )

            matched = results.count()

            response: Dict = {
                'type': 'FeatureCollection',
            }

            target_epsg = _get_target_epsg(
                crs_transform_spec, self.storage_crs)

            _add_geojson_crs(response, target_epsg)

            response['features'] = []
            response['numberMatched'] = matched
            response['numberReturned'] = 0

            if resulttype == 'hits' or not results:
                return response

            coord_trans = _get_coordinate_transformation(
                crs_transform_spec)

            items = results.order_by(
                *order_by_clauses).offset(offset).limit(limit)

            for item in items:
                response['numberReturned'] += 1
                response['features'].append(
                    self._sqlalchemy_to_feature_ext(item, target_epsg, coord_trans)
                )

        return response

    def get(self, identifier, crs_transform_spec=None, **kwargs):
        """
        Query the provider for a specific
        feature id e.g: /collections/hotosm_bdi_waterways/items/13990765

        :param identifier: feature id
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """
        start = time.time()

        # Execute query within self-closing database Session context
        with Session(self._engine) as session:
            # Retrieve data from database as feature
            item = session.get(self.table_model, identifier)

            if item is None:
                msg = f'No such item: {self.id_field}={identifier}.'
                raise ProviderItemNotFoundError(msg)

            target_epsg = _get_target_epsg(
                crs_transform_spec, self.storage_crs)
            coord_trans = _get_coordinate_transformation(
                crs_transform_spec)

            feature = self._sqlalchemy_to_feature_ext(
                item, target_epsg, coord_trans)

            _add_geojson_crs(feature, target_epsg)

            if self.properties:
                props: Dict = feature['properties']
                dropping_keys = deepcopy(props).keys()

                for item in dropping_keys:
                    if item not in self.properties:
                        props.pop(item)

            self._set_prev_and_next(identifier, feature, session)

        print(f'Got feature in {round(time.time() - start, 2)} sec.')

        return feature

    def _sqlalchemy_to_feature_ext(self, item, target_epsg: str, coord_trans: osr.CoordinateTransformation | None):
        feature: Dict = {
            'type': 'Feature'
        }

        item_dict: Dict = item.__dict__
        item_dict.pop('_sa_instance_state')

        if item_dict.get(self.geom):
            ewkb_elem: WKBElement = item_dict.pop(self.geom)
            wkb_elem = ewkb_elem.as_wkb()
            geom: ogr.Geometry = ogr.CreateGeometryFromWkb(wkb_elem.data)
            linear_geom: ogr.Geometry = geom.GetLinearGeometry()

            if coord_trans:
                linear_geom.Transform(coord_trans)

            if target_epsg == '4326':
                linear_geom.SwapXY()

            if target_epsg in ['4326', 'CRS84']:
                coord_precision = 'COORDINATE_PRECISION=6'
            else:
                coord_precision = 'COORDINATE_PRECISION=2'

            json_str = linear_geom.ExportToJson([coord_precision])
            geojson_geom = json.loads(json_str)

            feature['geometry'] = geojson_geom
        else:
            feature['geometry'] = None

        feature['id'] = item_dict.pop(self.id_field)
        feature['properties'] = item_dict

        return feature

    def _set_prev_and_next(self, identifier, feature: Dict, session: Session) -> None:
        ids = _get_table_ids(self.table_model, self.id_field, session)
        index = ids.index(identifier)

        if index + 1 == len(ids):
            next = ids[0]
        else:
            next = ids[index + 1]

        if index == 0:
            prev = ids[-1]
        else:
            prev = ids[index - 1]

        feature['prev'] = prev
        feature['next'] = next


def _get_coordinate_transformation(crs_transform_spec: CrsTransformSpec | None) -> osr.CoordinateTransformation | None:
    if not crs_transform_spec:
        return None

    source: osr.SpatialReference = osr.SpatialReference()
    source.ImportFromWkt(crs_transform_spec.source_crs_wkt)

    target: osr.SpatialReference = osr.SpatialReference()
    target.ImportFromWkt(crs_transform_spec.target_crs_wkt)

    return osr.CoordinateTransformation(source, target)


def _get_target_epsg(crs_transform_spec: CrsTransformSpec | None, storage_crs: str) -> str:
    if crs_transform_spec:
        return _get_epsg(crs_transform_spec.target_crs_wkt)

    return _get_epsg_from_uri(storage_crs)


def _add_geojson_crs(geojson: Dict, epsg: str) -> None:
    if epsg is None or epsg == 'CRS84':
        return

    geojson['crs'] = {
        'type': 'name',
        'properties': {
            'name': 'urn:ogc:def:crs:EPSG::' + epsg
        }
    }


def _get_epsg(wkt: str) -> str:
    sr: osr.SpatialReference = osr.SpatialReference()
    sr.ImportFromWkt(wkt)

    return sr.GetAuthorityCode(None)


def _get_epsg_from_uri(uri: str) -> str:
    return uri.split('/')[-1]


@cached(cache=_sessions_cache, key=lambda table_model, id_field, session: keys.hashkey(table_model))
def _get_table_ids(table_model, id_field, session: Session) -> List[Any]:
    id_column = getattr(table_model, id_field)
    result = session.query(id_column).order_by(id_column.asc())
    ids = [str(r[0]) for r in result]

    return ids

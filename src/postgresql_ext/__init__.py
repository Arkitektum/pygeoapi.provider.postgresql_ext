import json
from copy import deepcopy
import logging
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Any, Optional
from osgeo import ogr, osr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
from geoalchemy2 import WKBElement
from cachetools import cached, TTLCache, keys
import requests
from pygeoapi.provider.base import ProviderItemNotFoundError
from pygeoapi.provider.sql import PostgreSQLProvider
from pygeoapi.util import CrsTransformSpec, get_crs_from_uri

ogr.UseExceptions()
osr.UseExceptions()

_sessions_cache = TTLCache(maxsize=640*1024, ttl=86400)

LOGGER = logging.getLogger(__name__)


class PostgreSQLExtendedProvider(PostgreSQLProvider):
    """
    A provider for querying a PostgreSQL database. 
      * Supports nonlinear geometry types      
      * Supports field mappings from other tables or GML codelists
      * Caches table IDs for faster creation of fields for previous and next items
    """

    def __init__(self, provider_def: dict):
        super().__init__(provider_def)

        field_mappings = provider_def.get('field_mappings', [])
        namespace = self._get_collection_namespace()

        self.field_mapping_data = _get_field_mapping_data(field_mappings, namespace,
                                                          self._engine, self.db_search_path[0])
        self.navigation_templates = _normalize_navigation_config(
            provider_def.get('navigation'))

    def query(
        self,
        offset=0,
        limit=10,
        resulttype='results',
        bbox=[],
        datetime_=None,
        properties: List[Tuple[str, str]] = [],
        sortby: List[Dict[str, Any]] = [],
        select_properties: List[str] = [],
        skip_geometry=False,
        q=None,
        filterq=None,
        crs_transform_spec: Optional[CrsTransformSpec] = None,
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

        property_filters: Any = self._get_property_filters(properties)
        cql_filters: Any = self._get_cql_filters(filterq)
        bbox_filter: Any = self._get_bbox_filter(bbox)
        time_filter: Any = self._get_datetime_filter(datetime_)
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

            response: Dict[str, Any] = {
                'type': 'FeatureCollection'
            }

            crs_uri = crs_transform_spec.target_crs_uri if crs_transform_spec else self.storage_crs
            _add_geojson_crs(response, crs_uri)

            response['features'] = []
            response['numberMatched'] = matched
            response['numberReturned'] = 0

            if resulttype == 'hits' or not results:
                return response

            target_crs = _get_target_crs(
                crs_transform_spec, self.storage_crs)

            coord_trans = _get_coordinate_transformation(
                crs_transform_spec)

            items = results.order_by(
                *order_by_clauses).offset(offset).limit(limit)

            for item in items:
                response['numberReturned'] += 1
                response['features'].append(
                    self._create_feature(
                        item, target_crs, coord_trans)
                )

        return response

    def get(self, identifier, crs_transform_spec: Optional[CrsTransformSpec] = None, **kwargs):
        """
        Query the provider for a specific
        feature id e.g: /collections/hotosm_bdi_waterways/items/13990765

        :param identifier: feature id
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """

        with Session(self._engine) as session:
            item = session.get(self.table_model, identifier)

            if item is None:
                msg = f'No such item: {self.id_field}={identifier}.'
                raise ProviderItemNotFoundError(msg)

            target_crs = _get_target_crs(
                crs_transform_spec, self.storage_crs)

            coord_trans = _get_coordinate_transformation(
                crs_transform_spec)

            feature = self._create_feature(
                item, target_crs, coord_trans)

            crs_uri = crs_transform_spec.target_crs_uri if crs_transform_spec else self.storage_crs
            _add_geojson_crs(feature, crs_uri)

            if self.properties:
                props: Dict = feature['properties']
                dropping_keys = deepcopy(props).keys()

                for item in dropping_keys:
                    if item not in self.properties:
                        props.pop(item)

            self._set_prev_and_next(identifier, feature, session)

        return feature

    def _create_feature(self, item: Any, target_crs: str, coord_trans: osr.CoordinateTransformation | None) -> Dict[str, Any]:
        feature: Dict[str, Any] = {
            'type': 'Feature'
        }

        item_dict: Dict[str, Any] = item.__dict__
        item_dict.pop('_sa_instance_state')

        if item_dict.get(self.geom):
            ewkb_elem: WKBElement = item_dict.pop(self.geom)
            wkb_elem = ewkb_elem.as_wkb()
            geom: ogr.Geometry = ogr.CreateGeometryFromWkb(wkb_elem.data)
            linear_geom: ogr.Geometry = geom.GetLinearGeometry()

            if coord_trans:
                linear_geom.Transform(coord_trans)

            if target_crs == 'EPSG:4326':
                linear_geom.SwapXY()

            if target_crs in ['OGC:CRS84', 'EPSG:4326']:
                coord_precision = 'COORDINATE_PRECISION=6'
            else:
                coord_precision = 'COORDINATE_PRECISION=2'

            json_str = linear_geom.ExportToJson([coord_precision])

            feature['geometry'] = json.loads(json_str)
        else:
            feature['geometry'] = None

        feature_id = item_dict.pop(self.id_field)
        feature['id'] = feature_id

        self._add_mapped_values(item_dict)

        feature['properties'] = item_dict
        self._add_navigation_links(feature, feature_id)

        return feature

    def _set_prev_and_next(self, identifier, feature: Dict, session: Session) -> None:
        ids = _get_table_ids(self.table_model, self.id_field, session)
        index = ids.index(identifier)

        if index == 0:
            prev = ids[-1]
        else:
            prev = ids[index - 1]

        if index + 1 == len(ids):
            next = ids[0]
        else:
            next = ids[index + 1]

        feature['prev'] = prev
        feature['next'] = next

    def _add_mapped_values(self, item_dict: Dict) -> None:
        if not self.field_mapping_data:
            return

        for key, data in self.field_mapping_data.items():
            if not key in item_dict:
                continue

            value = item_dict[key]
            mapped_value = next(
                (tup for tup in data if tup[0] == str(value)), None)
            item_dict[key] = mapped_value[1] if mapped_value else value

    def _add_navigation_links(self, feature: Dict[str, Any], feature_id: Any) -> None:
        if not getattr(self, 'navigation_templates', None):
            return

        navigation: Dict[str, str] = {}
        format_context: Dict[str, Any] = {'id': feature_id}

        properties = feature.get('properties', {})

        if isinstance(properties, dict):
            format_context.update(properties)

        for tag, template in self.navigation_templates.items():
            try:
                navigation[tag] = template.format_map(format_context)
            except KeyError as err:
                missing = err.args[0]
                LOGGER.warning(
                    f'Navigation template "{tag}" for {self.id_field}={feature_id} is missing property "{missing}".')
            except Exception as err:
                LOGGER.warning(
                    f'Navigation template "{tag}" for {self.id_field}={feature_id} could not be resolved: {err}')

        if navigation:
            feature['navigation'] = navigation

    def _get_collection_namespace(self) -> str:
        return f'{self.db_name}.{self.db_search_path[0]}.{self.table}'


def _get_coordinate_transformation(crs_transform_spec: CrsTransformSpec | None) -> osr.CoordinateTransformation | None:
    if not crs_transform_spec:
        return None

    source: osr.SpatialReference = osr.SpatialReference()
    source.ImportFromWkt(crs_transform_spec.source_crs_wkt)

    target: osr.SpatialReference = osr.SpatialReference()
    target.ImportFromWkt(crs_transform_spec.target_crs_wkt)

    return osr.CoordinateTransformation(source, target)


def _get_target_crs(crs_transform_spec: CrsTransformSpec | None, storage_crs: str) -> str:
    return str(get_crs_from_uri(crs_transform_spec.target_crs_uri if crs_transform_spec else storage_crs))


def _add_geojson_crs(geojson: Dict[str, Any], crs_uri: str) -> None:
    crs = get_crs_from_uri(crs_uri)

    if crs.to_string() == 'OGC:CRS84':
        return

    geojson['crs'] = {
        'type': 'name',
        'properties': {
            'name': f'urn:ogc:def:crs:EPSG::{crs.to_epsg() or 4326}'
        }
    }


@cached(cache=_sessions_cache, key=lambda table_model, id_field, session: keys.hashkey(table_model))
def _get_table_ids(table_model, id_field, session: Session) -> List[Any]:
    id_column = getattr(table_model, id_field)
    result = session.query(id_column).order_by(id_column.asc())
    ids = [str(r[0]) for r in result]

    return ids


@cached(cache=_sessions_cache, key=lambda field_mappings, namespace, engine, db_search_path: keys.hashkey(namespace))
def _get_field_mapping_data(field_mappings: Dict[str, Dict[str, str]], namespace: str, engine: Engine, db_search_path: str) -> Dict[str, List[Tuple]]:
    mapping_data: Dict[str, List[Tuple]] = {}

    if not field_mappings:
        return mapping_data

    codelist_mappings = [
        item for item in field_mappings.items() if 'codelist' in item[1]]

    if codelist_mappings:
        codelist_mapping_data = _create_field_mapping_data_from_codelists(
            codelist_mappings)
        mapping_data.update(codelist_mapping_data)

    table_mappings = [item for item in field_mappings.items()
                      if 'table' in item[1]]

    if table_mappings:
        table_mapping_data = _create_field_mapping_data_from_tables(
            engine, db_search_path, table_mappings)
        mapping_data.update(table_mapping_data)

    return mapping_data


def _create_field_mapping_data_from_tables(engine: Engine, db_search_path: str, table_mappings: List[Tuple[str, Dict]]) -> Dict[str, List[Tuple]]:
    mapping_data: Dict[str, List[Tuple]] = {}

    with engine.connect() as connection:
        for key, value in table_mappings:
            try:
                sql = f'SELECT {value.get('id_field')}, {value.get('value_field')} FROM {db_search_path}.{value.get('table')}'
                result = connection.execute(text(sql))
                rows = result.fetchall()
                values = [tuple(row) for row in rows]
                mapping_data[key] = values
            except Exception as err:
                LOGGER.warning(
                    f'Could not create mapping data from table {value.get('table')}: {err}')

    return mapping_data


def _create_field_mapping_data_from_codelists(codelist_mappings: List[Tuple[str, Dict[str, str]]]) -> Dict[str, List[Tuple]]:
    mapping_data: Dict[str, List[Tuple]] = {}

    for key, value in codelist_mappings:
        url = value.get('codelist')

        if not url:
            continue

        try:
            mapping_data[key] = _get_codelist(url)
        except Exception as err:
            LOGGER.warning(
                f'Could not create mapping data from codelist {url}: {err}')

    return mapping_data


def _get_codelist(url: str) -> List[Tuple[str, str]]:
    response = requests.get(url)
    response.raise_for_status()

    root = ET.fromstring(response.text)
    ns = {'gml': 'http://www.opengis.net/gml/3.2'}
    codelist: List[Tuple[str, str]] = []

    for definition in root.findall('gml:dictionaryEntry/gml:Definition', ns):
        id = definition.findtext('gml:identifier', namespaces=ns)
        name = definition.findtext('gml:name', namespaces=ns)

        if not id or not name:
            continue

        codelist.append((id.strip(), name.strip()))

    codelist.sort(key=lambda entry: entry[0])

    return codelist


def _normalize_navigation_config(navigation_definition: Any) -> Dict[str, str]:
    templates: Dict[str, str] = {}

    if not navigation_definition:
        return templates

    if isinstance(navigation_definition, dict):
        for key, value in navigation_definition.items():
            if isinstance(value, str):
                templates[key] = value
                continue

            if isinstance(value, dict):
                template = value.get('template') or value.get(
                    'href') or value.get('path')

                if template:
                    templates[key] = template

        return templates

    if isinstance(navigation_definition, list):
        for item in navigation_definition:
            if not isinstance(item, dict):
                continue

            key = item.get('tag') or item.get('rel') or item.get('name')
            template = item.get('template') or item.get(
                'href') or item.get('path')

            if key and template:
                templates[key] = template

    return templates

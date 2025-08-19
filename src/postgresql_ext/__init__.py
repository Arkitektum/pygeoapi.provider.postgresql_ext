import os
import json
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import functools
import logging
from typing import Dict, Any
from geoalchemy2 import Geometry, WKBElement  # noqa - this isn't used explicitly but is needed to process Geometry columns
from geoalchemy2.functions import ST_MakeEnvelope
from geoalchemy2.shape import from_shape
from pygeofilter.backends.sqlalchemy.evaluate import to_filter
import pyproj
import shapely
from osgeo import ogr, osr
from sqlalchemy import create_engine, MetaData, PrimaryKeyConstraint, asc, desc, delete, text, select
from sqlalchemy.engine import URL
from sqlalchemy.exc import ConstraintColumnNotFoundError, InvalidRequestError, OperationalError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, load_only
from sqlalchemy.sql.expression import and_
from pygeoapi.provider.base import BaseProvider, ProviderConnectionError, ProviderInvalidDataError, ProviderQueryError, ProviderItemNotFoundError
from pygeoapi.util import get_transform_from_crs, CrsTransformSpec
from .helpers import get_coordinate_transformation, get_target_epsg, add_geojson_crs, get_table_ids

LOGGER = logging.getLogger(__name__)


class PostgreSQLExtendedProvider(BaseProvider):
    def __init__(self, provider_def):
        """
        PostgreSQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor

        :returns: pygeoapi.provider.base.PostgreSQLProvider
        """
        LOGGER.debug('Initialising PostgreSQL provider.')
        super().__init__(provider_def)

        self.table = provider_def['table']
        self.id_field = provider_def['id_field']
        self.geom = provider_def.get('geom_field', 'geom')

        self.storage_crs = provider_def.get(
            'storage_crs',
            'https://www.opengis.net/def/crs/OGC/0/CRS84'
        )

        options = None

        if provider_def.get('options'):
            options = provider_def['options']

        self._store_db_parameters(provider_def['data'], options)

        self._engine = get_engine(
            self.db_host,
            self.db_port,
            self.db_name,
            self.db_user,
            self._db_password,
            **(self.db_options or {})
        )

        self.table_model = get_table_model(
            self.table,
            self.id_field,
            self.db_search_path,
            self._engine
        )

        LOGGER.debug(f'DB connection: {repr(self._engine.url)}')
        self.get_fields()

    def query(self, offset=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None,
              filterq=None, crs_transform_spec: CrsTransformSpec | None = None, **kwargs):
        """
        Query Postgis for all the content.
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
        selected_properties = self._select_properties_clause(select_properties,
                                                             skip_geometry)

        with Session(self._engine) as session:
            results = (session.query(self.table_model)
                       .filter(property_filters)
                       .filter(cql_filters)
                       .filter(bbox_filter)
                       .filter(time_filter)
                       .options(selected_properties))

            matched = results.count()

            response: Dict = {
                'type': 'FeatureCollection',
            }

            target_epsg = get_target_epsg(crs_transform_spec, self.storage_crs)
            add_geojson_crs(response, target_epsg)

            response['features'] = []
            response['numberMatched'] = matched
            response['numberReturned'] = 0

            if resulttype == "hits" or not results:
                return response

            coord_trans = get_coordinate_transformation(
                crs_transform_spec)

            items = results.order_by(
                *order_by_clauses).offset(offset).limit(limit)

            for item in items:
                response['numberReturned'] += 1
                response['features'].append(
                    self._sqlalchemy_to_feature(item, target_epsg, coord_trans)
                )

        return response

    def get_fields(self):
        """
        Return fields (columns) from PostgreSQL table

        :returns: dict of fields
        """

        LOGGER.debug('Get available fields/properties')

        # sql-schema only allows these types, so we need to map from sqlalchemy
        # string, number, integer, object, array, boolean, null,
        # https://json-schema.org/understanding-json-schema/reference/type.html
        column_type_map = {
            bool: 'boolean',
            datetime: 'string',
            Decimal: 'number',
            dict: 'object',
            float: 'number',
            int: 'integer',
            str: 'string'
        }
        default_type = 'string'

        # https://json-schema.org/understanding-json-schema/reference/string#built-in-formats  # noqa
        column_format_map = {
            'date': 'date',
            'interval': 'duration',
            'time': 'time',
            'timestamp': 'date-time'
        }

        def _column_type_to_json_schema_type(column_type):
            try:
                python_type = column_type.python_type
            except NotImplementedError:
                LOGGER.warning(f'Unsupported column type {column_type}')
                return default_type
            else:
                try:
                    return column_type_map[python_type]
                except KeyError:
                    LOGGER.warning(f'Unsupported column type {column_type}')
                    return default_type

        def _column_format_to_json_schema_format(column_type):
            try:
                ct = str(column_type).lower()
                return column_format_map[ct]
            except KeyError:
                LOGGER.debug('No string format detected')
                return None

        if not self._fields:
            for column in self.table_model.__table__.columns:
                LOGGER.debug(f'Testing {column.name}')
                if column.name == self.geom:
                    continue

                self._fields[str(column.name)] = {
                    'type': _column_type_to_json_schema_type(column.type),
                    'format': _column_format_to_json_schema_format(column.type)
                }

        return self._fields

    def get(self, identifier, crs_transform_spec: CrsTransformSpec | None = None, **kwargs):
        """
        Query the provider for a specific
        feature id e.g: /collections/hotosm_bdi_waterways/items/13990765

        :param identifier: feature id
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """
        LOGGER.debug(f'Get item by ID: {identifier}')

        # Execute query within self-closing database Session context
        with Session(self._engine) as session:
            # Retrieve data from database as feature
            item = session.get(self.table_model, identifier)
            if item is None:
                msg = f"No such item: {self.id_field}={identifier}."
                raise ProviderItemNotFoundError(msg)

            target_epsg = get_target_epsg(
                crs_transform_spec, self.storage_crs)
            coord_trans = get_coordinate_transformation(
                crs_transform_spec)

            feature = self._sqlalchemy_to_feature(
                item, target_epsg, coord_trans)

            # Drop non-defined properties
            if self.properties:
                props = feature['properties']
                dropping_keys = deepcopy(props).keys()
                for item in dropping_keys:
                    if item not in self.properties:
                        props.pop(item)

            # Add fields for previous and next items
            self._set_prev_and_next(identifier, feature, session)

        return feature

    def create(self, item):
        """
        Create a new item

        :param item: `dict` of new item

        :returns: identifier of created item
        """

        identifier, json_data = self._load_and_prepare_item(
            item, accept_missing_identifier=True)

        new_instance = self._feature_to_sqlalchemy(json_data, identifier)
        with Session(self._engine) as session:
            session.add(new_instance)
            session.commit()
            result_id = getattr(new_instance, self.id_field)

        # NOTE: need to use id from instance in case it's generated
        return result_id

    def update(self, identifier, item):
        """
        Updates an existing item

        :param identifier: feature id
        :param item: `dict` of partial or full item

        :returns: `bool` of update result
        """

        identifier, json_data = self._load_and_prepare_item(
            item, raise_if_exists=False)

        new_instance = self._feature_to_sqlalchemy(json_data, identifier)
        with Session(self._engine) as session:
            session.merge(new_instance)
            session.commit()

        return True

    def delete(self, identifier):
        """
        Deletes an existing item

        :param identifier: item id

        :returns: `bool` of deletion result
        """
        with Session(self._engine) as session:
            id_column = getattr(self.table_model, self.id_field)
            result = session.execute(
                delete(self.table_model)
                .where(id_column == identifier)
            )
            session.commit()

        return result.rowcount > 0

    def _store_db_parameters(self, parameters: Dict, options):
        self.db_user = parameters.get('user')
        self.db_host = parameters.get('host')
        self.db_port = parameters.get('port', 5432)
        self.db_name = parameters.get('dbname')

        # db_search_path gets converted to a tuple here in order to ensure it
        # is hashable - which allows us to use functools.cache() when
        # reflecting the table definition from the DB

        search_path = parameters.get('search_path')

        if not search_path:
            self.db_search_path = tuple(['public'])
        elif isinstance(search_path, list):
            self.db_search_path = tuple(search_path)
        elif isinstance(search_path, str):
            schema_names = [schema_name.strip()
                            for schema_name in search_path.split(',')]
            self.db_search_path = tuple(schema_names)

        self._db_password = parameters.get('password')
        self.db_options = options

    def _sqlalchemy_to_feature(self, item, target_epsg: str, coord_trans: osr.CoordinateTransformation | None):
        feature: Dict = {
            'type': 'Feature'
        }

        item_dict: Dict = item.__dict__
        item_dict.pop('_sa_instance_state')
        feature['properties'] = item_dict
        feature['id'] = item_dict.pop(self.id_field)

        if feature['properties'].get(self.geom):
            ewkb_elem: WKBElement = feature['properties'].pop(self.geom)
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

        return feature

    def _feature_to_sqlalchemy(self, json_data, identifier=None):
        attributes = {**json_data['properties']}
        # 'identifier' key maybe be present in geojson properties, but might
        # not be a valid db field
        attributes.pop('identifier', None)
        srid: Any = pyproj.CRS.from_user_input(self.storage_crs).to_epsg()

        attributes[self.geom] = from_shape(
            shapely.geometry.shape(json_data['geometry']),
            # NOTE: for some reason, postgis in the github action requires
            # explicit crs information. i think it's valid to assume 4326:
            # https://portal.ogc.org/files/108198#feature-crs
            srid=srid
        )
        attributes[self.id_field] = identifier

        try:
            return self.table_model(**attributes)
        except Exception as e:
            LOGGER.exception('Failed to create db model')
            raise ProviderInvalidDataError(str(e))

    def _get_order_by_clauses(self, sort_by, table_model):
        # Build sort_by clauses if provided
        clauses = []
        for sort_by_dict in sort_by:
            model_column = getattr(table_model, sort_by_dict['property'])
            order_function = asc if sort_by_dict['order'] == '+' else desc
            clauses.append(order_function(model_column))

        # Otherwise sort by primary key (to ensure reproducible output)
        if not clauses:
            clauses.append(asc(getattr(table_model, self.id_field)))

        return clauses

    def _get_cql_filters(self, filterq):
        if not filterq:
            return True  # Let everything through

        # Convert filterq into SQL Alchemy filters
        field_mapping = {
            column_name: getattr(self.table_model, column_name)
            for column_name in self.table_model.__table__.columns.keys()}
        cql_filters = to_filter(filterq, field_mapping)

        return cql_filters

    def _get_property_filters(self, properties):
        if not properties:
            return True  # Let everything through

        # Convert property filters into SQL Alchemy filters
        # Based on https://stackoverflow.com/a/14887813/3508733
        filter_group = []
        for column_name, value in properties:
            column = getattr(self.table_model, column_name)
            filter_group.append(column == value)
        property_filters = and_(*filter_group)

        return property_filters

    def _get_bbox_filter(self, bbox):
        if not bbox:
            return True  # Let everything through

        # Convert bbx to SQL Alchemy clauses
        envelope = ST_MakeEnvelope(*bbox)
        geom_column = getattr(self.table_model, self.geom)
        bbox_filter = geom_column.intersects(envelope)

        return bbox_filter

    def _get_datetime_filter(self, datetime_):
        if datetime_ in (None, '../..'):
            return True
        else:
            if self.time_field is None:
                LOGGER.error('time_field not enabled for collection')
                raise ProviderQueryError()

            time_column = getattr(self.table_model, self.time_field)

            if '/' in datetime_:  # envelope
                LOGGER.debug('detected time range')
                time_begin, time_end = datetime_.split('/')
                if time_begin == '..':
                    datetime_filter = time_column <= time_end
                elif time_end == '..':
                    datetime_filter = time_column >= time_begin
                else:
                    datetime_filter = time_column.between(time_begin, time_end)
            else:
                datetime_filter = time_column == datetime_
        return datetime_filter

    def _select_properties_clause(self, select_properties, skip_geometry):
        # List the column names that we want
        if select_properties:
            column_names = set(select_properties)
        else:
            # get_fields() doesn't include geometry column
            column_names = set(self.fields.keys())

        if self.properties:  # optional subset of properties defined in config
            properties_from_config = set(self.properties)
            column_names = column_names.intersection(properties_from_config)

        if not skip_geometry:
            column_names.add(self.geom)

        # Convert names to SQL Alchemy clause
        selected_columns = []
        for column_name in column_names:
            try:
                column = getattr(self.table_model, column_name)
                selected_columns.append(column)
            except AttributeError:
                pass  # Ignore non-existent columns
        selected_properties_clause = load_only(*selected_columns)

        return selected_properties_clause

    def _get_crs_transform(self, crs_transform_spec=None):
        if crs_transform_spec is not None:
            crs_transform = get_transform_from_crs(
                pyproj.CRS.from_wkt(crs_transform_spec.source_crs_wkt),
                pyproj.CRS.from_wkt(crs_transform_spec.target_crs_wkt),
            )
        else:
            crs_transform = None
        return crs_transform

    def _set_prev_and_next(self, identifier, feature: Dict, session: Session) -> None:
        ids = get_table_ids(self.table_model, self.id_field, session)
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


@functools.cache
def get_engine(
        host: str,
        port: str,
        database: str,
        user: str,
        password: str,
        **connection_options
):
    """Create SQL Alchemy engine."""
    conn_str = URL.create(
        'postgresql+psycopg2',
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=database
    )
    conn_args = {
        'client_encoding': 'utf8',
        'application_name': 'pygeoapi',
        **connection_options,
    }
    engine = create_engine(
        conn_str,
        connect_args=conn_args,
        pool_pre_ping=True)
    return engine


@functools.cache
def get_table_model(
        table_name: str,
        id_field: str,
        db_search_path: tuple[str],
        engine,
):
    """Reflect table."""
    metadata = MetaData()

    # Look for table in the first schema in the search path
    schema = db_search_path[0]
    try:
        metadata.reflect(
            bind=engine, schema=schema, only=[table_name], views=True)
    except OperationalError:
        raise ProviderConnectionError(
            f"Could not connect to {repr(engine.url)} (password hidden).")
    except InvalidRequestError:
        raise ProviderQueryError(
            f"Table '{table_name}' not found in schema '{schema}' "
            f"on {repr(engine.url)}."
        )

    # Create SQLAlchemy model from reflected table
    # It is necessary to add the primary key constraint because SQLAlchemy
    # requires it to reflect the table, but a view in a PostgreSQL database
    # does not have a primary key defined.
    sqlalchemy_table_def = metadata.tables[f'{schema}.{table_name}']
    try:
        sqlalchemy_table_def.append_constraint(PrimaryKeyConstraint(id_field))
    except (ConstraintColumnNotFoundError, KeyError):
        raise ProviderQueryError(
            f"No such id_field column ({id_field}) on {schema}.{table_name}.")

    _Base = automap_base(metadata=metadata)
    _Base.prepare(
        name_for_scalar_relationship=_name_for_scalar_relationship,
    )
    return getattr(_Base.classes, table_name)


def _name_for_scalar_relationship(base, local_cls, referred_cls, constraint):
    """Function used when automapping classes and relationships from
    database schema and fixes potential naming conflicts.
    """
    name = referred_cls.__name__.lower()
    local_table = local_cls.__table__
    if name in local_table.columns:
        newname = name + '_'
        LOGGER.debug(
            f'Already detected column name {name!r} in table '
            f'{local_table!r}. Using {newname!r} for relationship name.'
        )
        return newname
    return name

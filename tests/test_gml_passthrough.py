"""Unit tests for GML passthrough (synthetic ST_AsGML columns).

Uses an in-memory SQLite automap model to exercise the real
mapper.add_property path; SQL is compiled (never executed), so the
PostGIS functions only need to render, not run.
"""

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import class_mapper
from sqlalchemy.sql import func

from postgresql_ext import (
    DERIVED_POINT_GML_KEY,
    GEOMETRY_GML_KEY,
    PROPERTY_SHAPE_DOTTED,
    PROPERTY_SHAPE_FLAT_LEAF,
    PROPERTY_SHAPE_NESTED,
    PostgreSQLExtendedProvider,
)


def _make_table_model():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    Table(
        "feat",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("identifikasjon.lokalId", String),
        Column("plantype", String),
        Column("geom", String),
    )
    metadata.create_all(engine)
    base = automap_base(metadata=metadata)
    base.prepare()

    return base.classes.feat


class _GmlStub(PostgreSQLExtendedProvider):
    """Provider stub that skips DB-backed init but attaches GML columns for real."""

    def __init__(
        self,
        table_model,
        *,
        gml_passthrough=True,
        derived_point_passthrough=False,
        gml_unwrap_multi=True,
        gml_options=1,
        gml_precision=15,
        property_shape=PROPERTY_SHAPE_DOTTED,
    ):
        self.table_model = table_model
        self.geom = "geom"
        self.id_field = "id"
        self.gml_passthrough = gml_passthrough
        self.derived_point_passthrough = derived_point_passthrough
        self.gml_unwrap_multi = gml_unwrap_multi
        self.gml_options = gml_options
        self.gml_precision = gml_precision
        self.property_shape = property_shape
        self.flatten_properties = property_shape == PROPERTY_SHAPE_FLAT_LEAF
        self._fields = {
            "identifikasjon.lokalId": {"type": "string", "format": None},
            "plantype": {"type": "string", "format": None},
        }
        self.excluded_properties = []
        self.properties = []
        self.field_mappings = {}

        synthetic_keys = []

        if gml_passthrough:
            synthetic_keys.append(GEOMETRY_GML_KEY)

            if derived_point_passthrough:
                synthetic_keys.append(DERIVED_POINT_GML_KEY)

        self._synthetic_keys = tuple(synthetic_keys)

        if gml_passthrough:
            self._attach_gml_columns()


class _FakeItem:
    def __init__(self, **values):
        self.__dict__.update(values)


def _make_item(**extra):
    return _FakeItem(
        _sa_instance_state=None,
        id=1,
        **{"identifikasjon.lokalId": "abc-123", "plantype": "35"},
        **extra,
    )


class TestAttachGmlColumns:
    def test_adds_geometry_gml_property(self):
        model = _make_table_model()
        _GmlStub(model)

        mapper = class_mapper(model)
        assert GEOMETRY_GML_KEY in mapper.attrs

        compiled = str(select(getattr(model, GEOMETRY_GML_KEY)))
        assert "ST_AsGML" in compiled
        assert "ST_GeometryN" in compiled  # unwrap CASE present

    def test_idempotent_under_repeated_construction(self):
        model = _make_table_model()
        _GmlStub(model)
        attrs_before = len(class_mapper(model).attrs)

        _GmlStub(model)  # second construction against same model

        assert len(class_mapper(model).attrs) == attrs_before

    def test_derived_point_adds_second_property(self):
        model = _make_table_model()
        _GmlStub(model, derived_point_passthrough=True)

        mapper = class_mapper(model)
        assert DERIVED_POINT_GML_KEY in mapper.attrs

        compiled = str(select(getattr(model, DERIVED_POINT_GML_KEY)))
        assert "ST_PointN" in compiled
        assert "ST_GeometryType" in compiled

    def test_unwrap_disabled_omits_geometryn(self):
        model = _make_table_model()
        _GmlStub(model, gml_unwrap_multi=False)

        compiled = str(select(getattr(model, GEOMETRY_GML_KEY)))
        assert "ST_AsGML" in compiled
        assert "ST_GeometryN" not in compiled

    def test_passthrough_off_attaches_nothing(self):
        model = _make_table_model()
        _GmlStub(model, gml_passthrough=False)

        mapper = class_mapper(model)
        assert GEOMETRY_GML_KEY not in mapper.attrs
        assert DERIVED_POINT_GML_KEY not in mapper.attrs

        compiled = str(select(model))
        assert "ST_AsGML" not in compiled


class TestQuerySqlShape:
    def test_count_statement_excludes_st_asgml(self):
        model = _make_table_model()
        _GmlStub(model)

        count_stmt = select(func.count(model.id))
        compiled = str(count_stmt)
        assert "ST_AsGML" not in compiled

    def test_select_properties_clause_loads_synthetics(self):
        model = _make_table_model()
        provider = _GmlStub(model)

        clause = provider._select_properties_clause(["plantype"])
        compiled = str(select(model).options(clause))
        assert "ST_AsGML" in compiled

    def test_select_properties_clause_without_passthrough_has_no_gml(self):
        model = _make_table_model()
        provider = _GmlStub(model, gml_passthrough=False)

        clause = provider._select_properties_clause(["plantype"])
        compiled = str(select(model).options(clause))
        assert "ST_AsGML" not in compiled


class TestSyntheticKeySurfacing:
    @pytest.mark.parametrize(
        "shape",
        [PROPERTY_SHAPE_DOTTED, PROPERTY_SHAPE_NESTED, PROPERTY_SHAPE_FLAT_LEAF],
    )
    def test_create_feature_surfaces_synthetics_top_level(self, shape):
        model = _make_table_model()
        provider = _GmlStub(model, property_shape=shape)
        item = _make_item(**{GEOMETRY_GML_KEY: "<gml:Polygon/>"})

        feature = provider._create_feature(item, "EPSG:25833", None, [], None)

        assert feature["properties"][GEOMETRY_GML_KEY] == "<gml:Polygon/>"

    def test_create_feature_surfaces_derived_point(self):
        model = _make_table_model()
        provider = _GmlStub(model, derived_point_passthrough=True)
        item = _make_item(
            **{
                GEOMETRY_GML_KEY: "<gml:LineString/>",
                DERIVED_POINT_GML_KEY: "<gml:Point/>",
            }
        )

        feature = provider._create_feature(item, "EPSG:25833", None, [], None)

        assert feature["properties"][DERIVED_POINT_GML_KEY] == "<gml:Point/>"

    def test_user_keys_still_shaped_dotted(self):
        model = _make_table_model()
        provider = _GmlStub(model, property_shape=PROPERTY_SHAPE_DOTTED)
        item = _make_item(**{GEOMETRY_GML_KEY: "<gml:Polygon/>"})

        feature = provider._create_feature(item, "EPSG:25833", None, [], None)

        assert feature["properties"]["identifikasjon.lokalId"] == "abc-123"

    def test_user_keys_still_shaped_nested(self):
        model = _make_table_model()
        provider = _GmlStub(model, property_shape=PROPERTY_SHAPE_NESTED)
        item = _make_item(**{GEOMETRY_GML_KEY: "<gml:Polygon/>"})

        feature = provider._create_feature(item, "EPSG:25833", None, [], None)

        assert feature["properties"]["identifikasjon"]["lokalId"] == "abc-123"
        # Synthetic key must NOT be nested.
        assert GEOMETRY_GML_KEY in feature["properties"]

    def test_get_properties_rejects_synthetic_selection(self):
        model = _make_table_model()
        provider = _GmlStub(model)

        keys = provider._get_properties([GEOMETRY_GML_KEY, "plantype"])

        assert keys == ["plantype"]

    def test_synthetics_absent_from_fields(self):
        model = _make_table_model()
        provider = _GmlStub(model)

        assert GEOMETRY_GML_KEY not in provider.fields
        assert GEOMETRY_GML_KEY not in provider._fields

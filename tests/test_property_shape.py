"""Unit tests for the property_shape config option.

The transformations are pure functions of dicts, so tests bypass DB
initialization by skipping the base provider's __init__ and setting only
the attributes the methods under test consult.
"""

import logging

import pytest

from postgresql_ext import (
    PROPERTY_SHAPE_DOTTED,
    PROPERTY_SHAPE_FLAT_LEAF,
    PROPERTY_SHAPE_NESTED,
    PostgreSQLExtendedProvider,
    _resolve_property_shape,
)


class _StubProvider(PostgreSQLExtendedProvider):
    """Provider stub that skips DB-backed init for pure-transform tests."""

    def __init__(self, property_shape: str, fields: dict | None = None):
        self.property_shape = property_shape
        self.flatten_properties = property_shape == PROPERTY_SHAPE_FLAT_LEAF
        self._fields = fields or {}
        self.field_mappings = {}


SAMPLE_FIELDS = {
    "id": {"type": "string", "format": None},
    "identifikasjon.lokalId": {"type": "string", "format": None},
    "identifikasjon.navnerom": {"type": "string", "format": None},
    "arealplanId.kommunenummer": {"type": "string", "format": None},
    "plantype": {"type": "integer", "format": None},
}

SAMPLE_PROPERTIES = {
    "identifikasjon.lokalId": "abc-123",
    "identifikasjon.navnerom": "https://example.test/ns",
    "arealplanId.kommunenummer": "0301",
    "plantype": 35,
}


class TestResolvePropertyShape:
    def test_default_is_nested(self):
        assert _resolve_property_shape({}) == PROPERTY_SHAPE_NESTED

    def test_explicit_dotted(self):
        assert (
            _resolve_property_shape({"property_shape": "dotted"})
            == PROPERTY_SHAPE_DOTTED
        )

    def test_explicit_flat_leaf(self):
        assert (
            _resolve_property_shape({"property_shape": "flat_leaf"})
            == PROPERTY_SHAPE_FLAT_LEAF
        )

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="property_shape must be one of"):
            _resolve_property_shape({"property_shape": "bogus"})

    def test_flatten_true_maps_to_flat_leaf(self, caplog):
        with caplog.at_level(logging.WARNING):
            shape = _resolve_property_shape({"flatten_properties": True})
        assert shape == PROPERTY_SHAPE_FLAT_LEAF
        # flatten_properties on its own is supported, not deprecated.
        assert caplog.records == []

    def test_flatten_false_maps_to_nested(self, caplog):
        with caplog.at_level(logging.WARNING):
            shape = _resolve_property_shape({"flatten_properties": False})
        assert shape == PROPERTY_SHAPE_NESTED
        assert caplog.records == []

    def test_explicit_wins_when_both_set(self, caplog):
        with caplog.at_level(logging.WARNING):
            shape = _resolve_property_shape(
                {"property_shape": "dotted", "flatten_properties": True}
            )
        assert shape == PROPERTY_SHAPE_DOTTED
        assert any("takes precedence" in r.message for r in caplog.records)


class TestShapeProperties:
    def test_dotted_passes_through_verbatim(self):
        provider = _StubProvider(PROPERTY_SHAPE_DOTTED)
        result = provider._shape_properties(SAMPLE_PROPERTIES)
        assert result == SAMPLE_PROPERTIES
        # Must be a new dict so callers can't mutate provider state.
        assert result is not SAMPLE_PROPERTIES

    def test_flat_leaf_strips_prefixes(self):
        provider = _StubProvider(PROPERTY_SHAPE_FLAT_LEAF)
        result = provider._shape_properties(SAMPLE_PROPERTIES)
        assert result == {
            "lokalId": "abc-123",
            "navnerom": "https://example.test/ns",
            "kommunenummer": "0301",
            "plantype": 35,
        }

    def test_nested_builds_objects(self):
        provider = _StubProvider(PROPERTY_SHAPE_NESTED)
        result = provider._shape_properties(SAMPLE_PROPERTIES)
        assert result == {
            "identifikasjon": {
                "lokalId": "abc-123",
                "navnerom": "https://example.test/ns",
            },
            "arealplanId": {"kommunenummer": "0301"},
            "plantype": 35,
        }


class TestFieldsProperty:
    def test_dotted_keeps_keys_verbatim(self):
        provider = _StubProvider(PROPERTY_SHAPE_DOTTED, fields=SAMPLE_FIELDS)
        assert provider.fields == SAMPLE_FIELDS

    def test_flat_leaf_uses_leaf_segment(self):
        provider = _StubProvider(PROPERTY_SHAPE_FLAT_LEAF, fields=SAMPLE_FIELDS)
        assert set(provider.fields.keys()) == {
            "id",
            "lokalId",
            "navnerom",
            "kommunenummer",
            "plantype",
        }

    def test_nested_builds_object_schemas(self):
        provider = _StubProvider(PROPERTY_SHAPE_NESTED, fields=SAMPLE_FIELDS)
        fields = provider.fields
        assert fields["id"] == {"type": "string", "format": None}
        assert fields["plantype"] == {"type": "integer", "format": None}
        assert fields["identifikasjon"]["type"] == "object"
        assert set(fields["identifikasjon"]["properties"].keys()) == {
            "lokalId",
            "navnerom",
        }
        assert set(fields["arealplanId"]["properties"].keys()) == {"kommunenummer"}

"""Unit tests for the warning about schema fields that have no column."""

import json
import logging
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

import postgresql_ext
from postgresql_ext import PostgreSQLProvider

SCHEMA = {
    "type": "object",
    "properties": {
        "objid": {"type": "integer"},
        "identifikasjon": {
            "type": "object",
            "properties": {"lokalId": {"type": "string"}},
        },
        "plantype": {"type": "string"},
    },
}


class _StubProvider(PostgreSQLProvider):
    """Skips DB-backed init; sets only what get_cached_fields consults."""

    def __init__(self, schema: str, *columns: str):
        self.schema = schema
        self.table = "rpomrade_omrade_mv"
        self._fields = {}
        self.table_model = SimpleNamespace(
            __table__=Table(
                "rpomrade_omrade_mv",
                MetaData(),
                Column("objid", Integer, primary_key=True),
                *(Column(name, String) for name in columns),
            )
        )


@pytest.fixture(autouse=True)
def _forget_reported_drift():
    postgresql_ext._reported_schema_drift.clear()
    yield
    postgresql_ext._reported_schema_drift.clear()


@pytest.fixture
def schema_path(tmp_path):
    path = tmp_path / "rpomrade.json"
    path.write_text(json.dumps(SCHEMA), encoding="utf-8")
    return str(path)


def test_schema_field_without_a_column_is_reported(schema_path, caplog):
    with caplog.at_level(logging.WARNING, logger="postgresql_ext.schema_drift"):
        _StubProvider(schema_path, "identifikasjon.lokalId").get_cached_fields

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "rpomrade_omrade_mv" in message
    assert "plantype" in message
    assert "identifikasjon.lokalId" not in message


def test_columns_outside_the_schema_are_not_reported(schema_path, caplog):
    with caplog.at_level(logging.WARNING, logger="postgresql_ext.schema_drift"):
        _StubProvider(
            schema_path, "identifikasjon.lokalId", "plantype", "rotasjon"
        ).get_cached_fields

    assert caplog.records == []


def test_drift_is_reported_once_per_table_and_schema(schema_path, caplog):
    with caplog.at_level(logging.WARNING, logger="postgresql_ext.schema_drift"):
        for _ in range(3):
            _StubProvider(schema_path, "identifikasjon.lokalId").get_cached_fields

    assert len(caplog.records) == 1


def test_fields_still_come_from_the_schema(schema_path):
    fields = _StubProvider(schema_path, "identifikasjon.lokalId").get_cached_fields

    assert set(fields) == {"objid", "identifikasjon.lokalId", "plantype"}


def test_drift_is_reported_when_the_root_logger_is_above_warning(
    schema_path, caplog
):
    root = logging.getLogger()
    previous = root.level
    root.setLevel(logging.ERROR)
    try:
        _StubProvider(schema_path, "plantype").get_cached_fields
    finally:
        root.setLevel(previous)

    assert len(caplog.records) == 1
    assert caplog.records[0].name == "postgresql_ext.schema_drift"

"""Unit tests for property-filter validation of extra query parameters."""

import pytest
from pygeoapi.provider.base import ProviderInvalidQueryError
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from postgresql_ext import PostgreSQLExtendedProvider

Base = declarative_base()


class _PlanomradeModel(Base):
    __tablename__ = "planomrade_test"

    objid = Column(Integer, primary_key=True)
    plannavn = Column(String)
    arealplan = Column(Integer)


@pytest.fixture
def provider():
    prov = PostgreSQLExtendedProvider.__new__(PostgreSQLExtendedProvider)
    prov.table_model = _PlanomradeModel
    return prov


def test_known_column_builds_filter(provider):
    filters = provider._get_property_filters([("arealplan", "123")])

    assert filters is not True  # a real SQLAlchemy clause, not the passthrough


def test_id_field_builds_filter(provider):
    # ?objid=1962 must keep working after unknown-param validation
    filters = provider._get_property_filters([("objid", "1962")])

    assert filters is not True
    assert "objid" in str(filters)


def test_empty_properties_pass_through(provider):
    assert provider._get_property_filters([]) is True


def test_unknown_parameter_raises_invalid_query(provider):
    # Regression: with include_extra_query_parameters enabled, pygeoapi
    # forwards any unknown query param (e.g. ?foo=1 or ?objid>1962) as a
    # property filter; getattr on the model raised AttributeError -> HTTP 500.
    # It must raise ProviderInvalidQueryError -> HTTP 400 instead.
    with pytest.raises(ProviderInvalidQueryError) as exc_info:
        provider._get_property_filters([("foo", "1")])

    assert "foo" in exc_info.value.message


def test_unparsed_cql_lookalike_raises_invalid_query(provider):
    # ?objid>1962 arrives as a parameter *named* "objid>1962" with empty value
    with pytest.raises(ProviderInvalidQueryError):
        provider._get_property_filters([("objid>1962", "")])

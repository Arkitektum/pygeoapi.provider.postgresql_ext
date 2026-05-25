"""Unit tests for the numberMatched count cache."""

from postgresql_ext import _count_cache, flush_count_cache


def test_flush_count_cache_empties_cache():
    _count_cache["k1"] = 100
    _count_cache["k2"] = 200
    assert len(_count_cache) == 2

    flush_count_cache()

    assert len(_count_cache) == 0


def test_flush_count_cache_idempotent_on_empty():
    _count_cache.clear()

    flush_count_cache()
    flush_count_cache()

    assert len(_count_cache) == 0

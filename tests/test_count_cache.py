"""Unit tests for the numberMatched count cache and signal-driven invalidation."""

import os

import postgresql_ext
from postgresql_ext import (
    _count_cache,
    _maybe_invalidate_from_signal,
    _sessions_cache,
    flush_caches,
    flush_count_cache,
)


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


def test_flush_caches_clears_both_caches():
    _count_cache["k1"] = 1
    _sessions_cache["k2"] = 2

    flush_caches()

    assert len(_count_cache) == 0
    assert len(_sessions_cache) == 0


def test_signal_newer_mtime_flushes(tmp_path, monkeypatch):
    monkeypatch.setattr(postgresql_ext, "_signal_mtime", 0.0)
    signal = tmp_path / "sync.signal"
    signal.touch()
    _count_cache["k"] = 42

    _maybe_invalidate_from_signal(str(signal))

    assert len(_count_cache) == 0
    assert postgresql_ext._signal_mtime == signal.stat().st_mtime


def test_signal_same_mtime_preserves_cache(tmp_path, monkeypatch):
    signal = tmp_path / "sync.signal"
    signal.touch()
    monkeypatch.setattr(postgresql_ext, "_signal_mtime", signal.stat().st_mtime)
    _count_cache["k"] = 42

    _maybe_invalidate_from_signal(str(signal))

    assert _count_cache["k"] == 42


def test_signal_missing_file_is_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(postgresql_ext, "_signal_mtime", 0.0)
    missing = tmp_path / "does-not-exist"
    _count_cache["k"] = 42

    _maybe_invalidate_from_signal(str(missing))

    assert _count_cache["k"] == 42
    assert postgresql_ext._signal_mtime == 0.0


def test_signal_second_touch_flushes_again(tmp_path, monkeypatch):
    monkeypatch.setattr(postgresql_ext, "_signal_mtime", 0.0)
    signal = tmp_path / "sync.signal"
    signal.touch()
    _maybe_invalidate_from_signal(str(signal))

    _count_cache["k"] = 99
    # Move mtime forward; os.utime is reliable across filesystems.
    new_mtime = signal.stat().st_mtime + 10
    os.utime(signal, (new_mtime, new_mtime))

    _maybe_invalidate_from_signal(str(signal))

    assert len(_count_cache) == 0


class _StubQuery:
    def __init__(self, value):
        self._value = value

    def filter(self, *_):
        return self

    def scalar(self):
        return self._value


class _StubSession:
    def __init__(self, value):
        self._value = value

    def query(self, *_):
        return _StubQuery(self._value)


def _count_with_filter(cql2_json: str, value: int) -> int:
    from pygeofilter.parsers.cql2_json import parse as parse_cql2_json

    ast = parse_cql2_json(cql2_json)
    return postgresql_ext._get_matched_count(
        "public.planomrade",
        (),
        (),
        None,
        ast,
        _StubSession(value),
        None,
        True,
        True,
        True,
        True,
    )


def test_count_cache_accepts_pygeofilter_ast():
    # Regression: pygeofilter AST nodes are unhashable dataclasses; the cache
    # key must not hash them directly (TypeError in cachetools.keys).
    flush_count_cache()

    count = _count_with_filter('{"op": ">", "args": [{"property": "objid"}, 1962]}', 7)

    assert count == 7


def test_count_cache_distinguishes_filters_and_hits_on_equal_ast():
    flush_count_cache()

    first = _count_with_filter('{"op": ">", "args": [{"property": "objid"}, 1962]}', 7)
    other = _count_with_filter('{"op": ">", "args": [{"property": "objid"}, 9999]}', 8)
    # Re-parsed-but-equal filter must hit the cache: stub value 99 is ignored.
    cached_ = _count_with_filter(
        '{"op": ">", "args": [{"property": "objid"}, 1962]}', 99
    )

    assert (first, other, cached_) == (7, 8, 7)

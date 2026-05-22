"""Pytest fixtures for the postgresql_ext test suite.

The production module imports `osgeo` eagerly. For unit tests covering
pure-Python transformations (no GDAL needed), we stub the package so the
module imports cleanly without the system GDAL bindings installed.
"""

import sys
import types


def _install_osgeo_stub() -> None:
    if "osgeo" in sys.modules:
        return

    osgeo = types.ModuleType("osgeo")
    ogr = types.ModuleType("osgeo.ogr")
    osr = types.ModuleType("osgeo.osr")

    for name, value in (
        ("UseExceptions", lambda: None),
        ("Geometry", type("Geometry", (), {})),
        ("CreateGeometryFromWkb", lambda *_a, **_kw: None),
    ):
        setattr(ogr, name, value)

    for name, value in (
        ("UseExceptions", lambda: None),
        ("CoordinateTransformation", type("CoordinateTransformation", (), {})),
        ("SpatialReference", type("SpatialReference", (), {})),
    ):
        setattr(osr, name, value)

    setattr(osgeo, "ogr", ogr)
    setattr(osgeo, "osr", osr)

    sys.modules["osgeo"] = osgeo
    sys.modules["osgeo.ogr"] = ogr
    sys.modules["osgeo.osr"] = osr


_install_osgeo_stub()

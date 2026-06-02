# PostgreSQL Extended Provider

Extended PostgreSQL provider for [pygeoapi](https://pygeoapi.io/) with
support for:

- nonlinear geometry types via GDAL
- field/value mappings from lookup tables or GML codelists
- cached lookups for previous/next items within a collection
- templated related links surfaced through the feature `links` array
- selectable property shape (`dotted` | `nested` | `flat_leaf`) for
  collections whose columns use dot-separated naming

## Property shape

Set `property_shape` in your provider definition to control how columns
with dot-separated names (e.g. `identifikasjon.lokalId`) appear in
feature properties and in the queryable schema.

| Mode | Output keys for `identifikasjon.lokalId` | Use case |
| --- | --- | --- |
| `nested` (default) | `{"identifikasjon": {"lokalId": ...}}` | GeoJSON consumers expecting nested objects |
| `flat_leaf` | `lokalId` | Legacy / flat consumers; collisions overwrite |
| `dotted` | `identifikasjon.lokalId` | Required by the GML formatter contract |

```yaml
providers:
  - type: feature
    name: postgresql_ext.PostgreSQLExtendedProvider
    property_shape: dotted
```

The same shape is applied to `/queryables`, so filter expressions match
the form clients see in feature properties.

`flatten_properties: true|false` is an equivalent, fully supported
shorthand for the two binary modes (`true` → `flat_leaf`, `false` →
`nested`). Use it when those two modes cover your needs;
`property_shape` is the more expressive form when `dotted` is required.
If both keys are set on the same provider, `property_shape` takes
precedence and a warning is logged.

## Cache invalidation

`numberMatched` counts and prev/next id sets are cached in-process with a
24-hour TTL. To force invalidation after a data swap (e.g. pg-data-sync),
configure a sentinel file the provider watches:

```yaml
providers:
  - type: feature
    name: postgresql_ext.PostgreSQLExtendedProvider
    cache_signal_path: /var/run/pygeoapi/sync.signal
```

The provider checks the file's mtime at the start of every `query()` and
`get()`. When the mtime advances, both `_count_cache` and `_sessions_cache`
are cleared. The sync process triggers invalidation with a single touch:

```python
from pathlib import Path
Path("/var/run/pygeoapi/sync.signal").touch()
```

The file does not need to exist at startup. A missing file is a no-op.
Cost per request is one `os.stat()` (kernel-cached, sub-microsecond).
Each pygeoapi worker process tracks its own last-seen mtime, so a single
touch invalidates caches across every worker that shares the volume.

For same-process invalidation (tests, in-process operators), call
`postgresql_ext.flush_caches()` directly.

## GML passthrough

For collections consumed by a GML formatter (e.g.
`pygeoapi.formatter.gml-npad`), the provider can attach server-rendered
GML 3.2 geometry strings as synthetic feature properties, computed by
PostGIS `ST_AsGML` at query time:

```yaml
providers:
  - type: feature
    name: postgresql_ext.PostgreSQLExtendedProvider
    property_shape: dotted
    gml_passthrough: true
    derived_point_passthrough: true   # only for collections needing label points
    gml_options: 1                    # ST_AsGML options bitmask (default 1: long CRS URN, curve-aware)
    gml_precision: 15                 # max decimal digits (default 15)
    gml_unwrap_multi: true            # unwrap single-component Multi* (default true)
```

With `gml_passthrough: true` every feature gains a `_geometry_gml`
property containing the raw `ST_AsGML(3, geom, precision, options)`
output. Single-component Multi* geometries are unwrapped to their only
member first (`gml_unwrap_multi`), since several application schemas
reject Multi* wrappers in singular property types.
`derived_point_passthrough: true` adds `_derived_point_gml`: the start
point for line geometries, the geometry itself for points.

The synthetic keys are contract keys, not user data: they bypass
`property_shape`, always appear top-level in `properties`, are excluded
from `/queryables`, and cannot be requested via `?properties=`. They are
not available for filtering or sorting.

Use `gml_options: 5` (adds flag 4) to emit linearized
`LineString`/`Polygon` instead of curve-aware `Curve`/`Surface` output.

Note: geometries are not validated. Invalid source geometries serialize
to invalid GML — data quality is upstream of this provider.

## Related links between collections

Use the optional `links` block in your provider definition to expose links that
point to related collections. Each entry is rendered with `str.format`, so you
can reference the feature identifier (`{id}`) or any property returned for the
feature.

```yaml
providers:
  - type: feature
    name: postgresql_ext.PostgreSQLExtendedProvider
    data:
      id_field: your_id
      table: parent_collection_table
      links_base: "https://example.com/vorpah/"
      links:
        - rel: related
          href: "collections/child-collection/items?foreignKey={foreign_key_prop}"
          title: "child"
        - rel: related
          href: "collections/parent-collection/items/{id}"
          title: "Parent for {id}"
```

When features are returned (both for `query` and `get` requests) every link
template renders an entry in the feature `links` array. The rendered template is
resolved to an absolute `href`, with defaults `rel: "related"` and
`type: "application/json"` unless you override them. You can also supply custom
titles or other metadata; nested dictionaries and lists are rendered
recursively.

```json
{
  "type": "Feature",
  "id": "123",
  "geometry": { "...": "..." },
  "properties": { "...": "..." },
  "links": [
    {
      "rel": "related",
      "href": "collections/child-collection/items?foreignKey=42",
      "title": "child",
      "type": "application/json"
    },
    {
      "rel": "related",
      "href": "collections/parent-collection/items/123",
      "title": "parent",
      "type": "application/json"
    }
  ]
}
```

If a template references an unknown property, the entry is skipped and a warning
is logged. This allows you to define different related links per collection
without breaking responses when some attributes are missing. Use any `rel`
values that make sense for your API; `related` is the default when none is
provided. To force a specific base URL (for example when running behind a
reverse proxy) supply `links_base` in the provider configuration; otherwise the
current request URL is used.

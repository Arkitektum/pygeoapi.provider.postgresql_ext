# PostgreSQL Extended Provider

Extended PostgreSQL provider for [pygeoapi](https://pygeoapi.io/) with
support for:

- nonlinear geometry types via GDAL
- field/value mappings from lookup tables or GML codelists
- cached lookups for previous/next items within a collection
- configurable navigation links to related collections

## Navigation links between collections

Use the optional `navigation` block in your provider definition to expose
links that point to related collections. Each entry is rendered with
`str.format`, so you can reference the feature identifier (`{id}`) or any
property name returned for the feature.

```yaml
providers:
  - type: feature
    name: postgresql_ext.PostgreSQLExtendedProvider
    data:
      id_field: your_id
      table: parent_collection_table
      navigation:
        child: "collections/child-collection/items?foreignKey={foreign_key_prop}"
        parent: "collections/parent-collection/items/{id}"
```

When features are returned (both for `query` and `get` requests) an extra
`navigation` member is added alongside the standard GeoJSON members. Each
navigation entry is also appended to the feature `links` array. Links whose
rendered template includes a query string (`?`) receive `rel: "items"`, others
receive `rel: "item"`. The navigation key is persisted as the link `title`, and
the rendered template becomes the `href` (default media type
`application/json`):

```json
{
  "type": "Feature",
  "id": "123",
  "geometry": { "...": "..." },
  "properties": { "...": "..." },
  "navigation": {
    "child": "collections/child-collection/items?foreignKey=42",
    "parent": "collections/parent-collection/items/123"
  }
}
```

If a template references an unknown property, the entry is skipped and a
warning is logged. This allows you to define different navigation targets
per collection without breaking responses when some attributes are missing.
Navigation keys (`child`, `parent`, etc.) are arbitrary—use any names that
best describe the related links you want to expose.

# PostgreSQL Extended Provider

Extended PostgreSQL provider for [pygeoapi](https://pygeoapi.io/) with
support for:

- nonlinear geometry types via GDAL
- field/value mappings from lookup tables or GML codelists
- cached lookups for previous/next items within a collection
- templated related links surfaced through the feature `links` array

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
      links:
        - rel: related
          href: "collections/child-collection/items?foreignKey={foreign_key_prop}"
          title: "child"
        - rel: related
          href: "collections/parent-collection/items/{id}"
          title:
            en: "Parent for {id}"
            nb: "Forelder for {id}"
```

When features are returned (both for `query` and `get` requests) every link
template renders an entry in the feature `links` array. The rendered template
becomes the `href`, with defaults `rel: "related"` and `type: "application/json"`
unless you override them. You can also supply translated titles or other
metadata; nested dictionaries and lists are rendered recursively.

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
provided.

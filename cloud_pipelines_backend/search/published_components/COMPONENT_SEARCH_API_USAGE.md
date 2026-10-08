# Component Search API — Usage Guide

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/published_components/experimental/search` | Search published components |
| `GET`  | `/api/published_components/experimental/search/schema` | Discover searchable fields |

---

## Semantic Search Availability

Text and keyword search (filters, predicates) always work regardless of configuration. Semantic search requires a configured embedding provider.

When the embedding provider is **not configured**:

- **Search**: Filter-only queries succeed normally. Requests that include `semantic` return `422 Unprocessable Entity`.
- **Schema**: The response omits semantic fields and includes a `notices` array explaining why.

```json
{
  "fields": [
    {"path": "name", "type": "text", "description": "Component name"},
    {"path": "published_by", "type": "text", "description": "Publisher email"}
  ],
  "total_indexed": 200,
  "notices": ["Semantic search is unavailable (embedding provider not configured)."]
}
```

---

## Search API

### Request body

```json
{
  "filter": { ... },
  "semantic": [ ... ],
  "fields": ["digest", "name", "published_by"],
  "size": 20,
  "sort_order": "desc",
  "page_token": null
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `filter` | object \| null | `null` | Filter predicates (see below) |
| `semantic` | array \| null | `null` | Semantic / KNN searches |
| `fields` | array \| null | `null` | ES `_source` fields to return. `null` returns `["digest", "name", "published_by"]`. Add `"spec"` to include the full component spec (large). |
| `size` | int | `20` | Results per page (1–500) |
| `sort_order` | `"asc"` \| `"desc"` | `"desc"` | Sort direction for relevance score |
| `page_token` | object \| null | `null` | Pagination cursor from a previous response |

### Response body

```json
{
  "results": [
    {
      "digest": "abc123...",
      "name": "Filter columns",
      "published_by": "alice@example.com",
      "score": 0.95
    }
  ],
  "total": 42,
  "next_page_token": {"search_after": [0.72, "def456"]}
}
```

> **Note:** `(digest, published_by)` is the composite primary key for a published component. Use both to uniquely identify a component when fetching full details.

---

## Use Cases

### Search Predicates

| Predicate | ES Query | Case-Sensitive | Description | Example |
|-----------|----------|----------------|-------------|---------|
| `value_match` | `match` | Key: yes, Value: no | Smart word search with stemming and tokenization. | "training models" matches "Train XGBoost model" |
| `value_fuzzy` | `fuzzy` (fuzziness: AUTO) | Key: yes, Value: no | Typo-tolerant search. Fuzziness auto-scales by word length. | "filtr" matches "filter", "xgbost" matches "xgboost" |
| `value_equals` | `term` (on `.keyword`) | Yes | Exact match on a keyword field. | "Ark-kun" matches only "Ark-kun", not "ark-kun" |
| `value_contains` | `wildcard` (on `.keyword`) | Yes | Substring match anywhere in the field. | "xgb" matches "Train XGBoost model" |
| `value_in` | `terms` (on `.keyword`) | Yes | Match any of several exact values. | ["alice", "bob"] matches "alice" or "bob" |
| `value_regex` | `regexp` (on `.keyword`) | Yes | Full regex pattern match on exact field value. | `Filter.*v[0-9]+` matches "Filter columns v2" |
| `value_equals_case_insensitive` | `term` (on `.keyword`, case_insensitive) | Key: yes, Value: no | Exact match, ignoring case. | "ark-kun" matches "Ark-kun", "ARK-KUN" |
| `value_contains_case_insensitive` | `wildcard` (on `.keyword`, case_insensitive) | Key: yes, Value: no | Substring match, ignoring case. | "xgb" matches "Train XGBoost Model" |
| `value_in_case_insensitive` | `bool.should` of `term` (case_insensitive) | Key: yes, Value: no | Match any of several values, ignoring case. | ["Alice", "bob"] matches "alice", "BOB" |
| `value_regex_case_insensitive` | `regexp` (on `.keyword`, case_insensitive) | Key: yes, Value: no | Regex match, ignoring case. | `filter.*` matches "Filter columns v2" |
| `key_exists` | `exists` | Yes | Check if a field is present and non-null. | Components that have a `spec.description` |

### Query Operators & Semantic

| Predicate | ES Query | Description | Example |
|-----------|----------|-------------|---------|
| `and` | `bool.must` | All predicates must match. | Name contains "filter" AND publisher is "alice" |
| `or` | `bool.should` (minimum_should_match: 1) | At least one predicate must match. | Name matches "filter" OR name matches "transform" |
| `not` | `bool.must_not` | Negate a leaf predicate. | NOT published by "bot@example.com" |
| `semantic` | `knn` (query_vector) | KNN vector similarity search (root-level, not inside filters). | "clean and preprocess data" finds related components by meaning |

---

## Filter Predicates

All predicates are nestable inside `"and"` / `"or"` / `"not"` operators.

### `value_equals` — exact match

ES query: `term` on `.keyword`

```json
// API predicate
{"value_equals": {"key": "published_by", "value": "alice@example.com"}}

// Translated ES query
{"term": {"published_by.keyword": "alice@example.com"}}
```

### `value_contains` — substring search

ES query: `wildcard` on `.keyword` (wraps value in `*...*`)

```json
// API predicate
{"value_contains": {"key": "name", "value_substring": "filter"}}

// Translated ES query
{"wildcard": {"name.keyword": "*filter*"}}
```

### `value_in` — match any of several values

ES query: `terms` on `.keyword`

```json
// API predicate
{"value_in": {"key": "published_by", "values": ["alice@example.com", "bob@example.com"]}}

// Translated ES query
{"terms": {"published_by.keyword": ["alice@example.com", "bob@example.com"]}}
```

### `key_exists` — field presence check

ES query: `exists`

```json
// API predicate
{"key_exists": {"key": "spec.description"}}

// Translated ES query
{"exists": {"field": "spec.description"}}
```

### `value_match` — smart word search (stemming, analyzed)

ES query: `match` (uses the `text` field, not `.keyword`)

Matches individual words with stemming. "training models" matches "Train XGBoost model".

```json
// API predicate
{"value_match": {"key": "name", "query": "train xgboost model"}}

// Translated ES query
{"match": {"name": "train xgboost model"}}
```

### `value_fuzzy` — typo-tolerant search

ES query: `fuzzy` with `fuzziness: "AUTO"` (uses the `text` field, not `.keyword`)

Handles typos. "filtr" matches "filter". Fuzziness is `AUTO` (internally hardcoded).

```json
// API predicate
{"value_fuzzy": {"key": "name", "value": "filtr"}}

// Translated ES query
{"fuzzy": {"name": {"value": "filtr", "fuzziness": "AUTO"}}}
```

### `value_regex` — regular expression

ES query: `regexp` on `.keyword`

```json
// API predicate
{"value_regex": {"key": "name", "pattern": "Filter.*v[0-9]+"}}

// Translated ES query
{"regexp": {"name.keyword": "Filter.*v[0-9]+"}}
```

### `value_equals_case_insensitive` — case-insensitive exact match

ES query: `term` on `.keyword` with `case_insensitive: true`

```json
// API predicate
{"value_equals_case_insensitive": {"key": "published_by", "value": "alice@example.com"}}

// Translated ES query
{"term": {"published_by.keyword": {"value": "alice@example.com", "case_insensitive": true}}}
```

### `value_contains_case_insensitive` — case-insensitive substring search

ES query: `wildcard` on `.keyword` with `case_insensitive: true`

```json
// API predicate
{"value_contains_case_insensitive": {"key": "name", "value_substring": "filter"}}

// Translated ES query
{"wildcard": {"name.keyword": {"value": "*filter*", "case_insensitive": true}}}
```

### `value_in_case_insensitive` — case-insensitive match any of several values

ES query: `bool.should` of `term` with `case_insensitive: true` (ES `terms` does not support `case_insensitive`, so each value is expanded into an individual `term` query)

```json
// API predicate
{"value_in_case_insensitive": {"key": "published_by", "values": ["alice@example.com", "bob@example.com"]}}

// Translated ES query
{
  "bool": {
    "should": [
      {"term": {"published_by.keyword": {"value": "alice@example.com", "case_insensitive": true}}},
      {"term": {"published_by.keyword": {"value": "bob@example.com", "case_insensitive": true}}}
    ],
    "minimum_should_match": 1
  }
}
```

### `value_regex_case_insensitive` — case-insensitive regex

ES query: `regexp` on `.keyword` with `case_insensitive: true`

```json
// API predicate
{"value_regex_case_insensitive": {"key": "name", "pattern": "filter.*v[0-9]+"}}

// Translated ES query
{"regexp": {"name.keyword": {"value": "filter.*v[0-9]+", "case_insensitive": true}}}
```

### Boolean operators

**AND** — ES query: `bool.must` (all must match)

```json
// API predicate
{
  "filter": {
    "and": [
      {"value_match": {"key": "name", "query": "train model"}},
      {"value_equals": {"key": "published_by", "value": "alice@example.com"}}
    ]
  }
}

// Translated ES query
{
  "query": {
    "bool": {
      "must": [
        {"match": {"name": "train model"}},
        {"term": {"published_by.keyword": "alice@example.com"}}
      ]
    }
  }
}
```

**OR** — ES query: `bool.should` with `minimum_should_match: 1` (at least one must match)

```json
// API predicate
{
  "filter": {
    "or": [
      {"value_fuzzy": {"key": "name", "value": "filtr"}},
      {"value_regex": {"key": "name", "pattern": "Filter.*"}}
    ]
  }
}

// Translated ES query
{
  "query": {
    "bool": {
      "should": [
        {"fuzzy": {"name": {"value": "filtr", "fuzziness": "AUTO"}}},
        {"regexp": {"name.keyword": "Filter.*"}}
      ],
      "minimum_should_match": 1
    }
  }
}
```

**NOT** — ES query: `bool.must_not` (negate a leaf predicate)

```json
// API predicate
{
  "filter": {
    "and": [
      {"not": {"value_equals": {"key": "published_by", "value": "bot@example.com"}}},
      {"value_match": {"key": "name", "query": "filter"}}
    ]
  }
}

// Translated ES query
{
  "query": {
    "bool": {
      "must": [
        {"bool": {"must_not": [{"term": {"published_by.keyword": "bot@example.com"}}]}},
        {"match": {"name": "filter"}}
      ]
    }
  }
}
```

---

## Semantic Search

Semantic search uses KNN vector similarity. It is a **root-level** concept — not nested inside filter predicates.

Each semantic search entry has:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `field` | string | — | The vector field name to search |
| `query` | string | — | The text to embed and search for |
| `k` | int | `20` | Number of nearest neighbors to find (1–500). Independent of `size`. |

### Semantic field aliases

The API supports **friendly aliases** for semantic vector fields. Instead of the full Elasticsearch field name, you can use a short alias:

| Alias | Full ES field |
|-------|---------------|
| `name_and_description` | `name_and_description_vector__openai_text-embedding-3-large__3072` |
| `full_spec` | `full_spec_vector__openai_text-embedding-3-large__3072` |

The **Schema API** returns aliases as the `path` for semantic fields. The **Search API** accepts both aliases and full ES field names (backward-compatible).

### Single semantic search

ES query: `knn` with `query_vector` (the API embeds the text automatically)

```json
// API predicate (using alias)
{
  "semantic": [
    {"field": "full_spec", "knn_query": "clean and filter data", "knn_k": 50}
  ]
}

// Translated ES query (alias resolved to full ES field name)
{
  "knn": {
    "field": "full_spec_vector__openai_text-embedding-3-large__3072",
    "query_vector": [0.012, -0.034, ...],
    "k": 50
  }
}
```

### Hybrid (filters + semantic)

ES query: combines `bool.must` (filter) + `knn` (semantic) at the same level

```json
// API predicate
{
  "filter": {
    "and": [
      {"value_equals": {"key": "published_by", "value": "alice@example.com"}}
    ]
  },
  "semantic": [
    {"field": "name_and_description", "knn_query": "data preprocessing", "knn_k": 100}
  ]
}

// Translated ES query
{
  "query": {
    "bool": {
      "must": [
        {"term": {"published_by.keyword": "alice@example.com"}}
      ]
    }
  },
  "knn": {
    "field": "name_and_description_vector__openai_text-embedding-3-large__3072",
    "query_vector": [0.012, -0.034, ...],
    "k": 100
  }
}
```

Use the **Schema API** to discover available semantic field aliases.

---

## Pagination

Uses ES `search_after` for cursor-based pagination.

### How it works

1. Send a query **without** `page_token` to get the first page.
2. If the response includes `"next_page_token"`, pass it as `"page_token"` in the next request.
3. Repeat until `"next_page_token"` is `null` (no more pages).

### Page 1 — no `page_token`

**Request:**

```json
{
  "filter": {"and": [{"value_match": {"key": "name", "query": "filter"}}]},
  "size": 5
}
```

**Response:**

```json
{
  "results": [
    {"digest": "abc", "name": "Filter columns", "published_by": "alice@example.com", "score": 0.95},
    {"digest": "def", "name": "Filter rows",    "published_by": "bob@example.com",   "score": 0.72}
  ],
  "total": 12,
  "next_page_token": {"search_after": [0.72, "def"]}
}
```

### Page 2 — pass previous `next_page_token`

**Request:**

```json
{
  "filter": {"and": [{"value_match": {"key": "name", "query": "filter"}}]},
  "size": 5,
  "page_token": {"search_after": [0.72, "def"]}
}
```

### Last page — `next_page_token` is `null`

```json
{
  "results": [ ... ],
  "total": 12,
  "next_page_token": null
}
```

### Python pagination example

```python
import requests

url = "http://localhost:8000/api/published_components/experimental/search"
query = {
    "filter": {"and": [{"value_match": {"key": "name", "query": "filter"}}]},
    "size": 50,
    "sort_order": "desc",
}
all_results = []

while True:
    response = requests.post(url, json=query).json()
    all_results.extend(response["results"])
    if response["next_page_token"] is None:
        break
    query["page_token"] = response["next_page_token"]

print(f"Fetched {len(all_results)} of {response['total']} total results")
```

---

## Schema API

Discover all searchable fields, their types, and descriptions.

### Request

```
GET /api/published_components/experimental/search/schema
```

### Response

```json
{
  "fields": [
    {"path": "name",            "type": "text",     "description": "Component name"},
    {"path": "name.keyword",    "type": "keyword",  "description": null},
    {"path": "published_by",    "type": "text",     "description": "Publisher email"},
    {"path": "digest",          "type": "text",     "description": "Component content hash"},
    {"path": "spec.description","type": "text",     "description": null},
    {"path": "spec.inputs.name","type": "text",     "description": null},
    {"path": "name_and_description",
     "type": "semantic",
     "description": "Semantic search: name + description"},
    {"path": "full_spec",
     "type": "semantic",
     "description": "Semantic search: full component spec"}
  ],
  "total_indexed": 492,
  "notices": null
}
```

> **Note:** Semantic field paths are friendly aliases (e.g. `name_and_description`, `full_spec`). Use these aliases in the `semantic[].field` parameter of search requests.

### Field types and supported predicates

| Type | ES types | Supported predicates |
|------|----------|---------------------|
| `text` | `text` | `value_match`, `value_fuzzy`, `value_contains`, `value_regex` |
| `keyword` | `keyword` | `value_equals`, `value_in`, `value_regex`, `value_contains` |
| `semantic` | `dense_vector`, `sparse_vector`, `semantic_text` | `semantic` search only (root-level) |

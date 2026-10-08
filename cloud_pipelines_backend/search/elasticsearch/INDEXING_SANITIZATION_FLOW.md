# Indexing Sanitization Flow

When Elasticsearch has dynamically mapped a field as `text` but a subsequent document sends a dict/list for that field, a `document_parsing_exception` is thrown. The indexing loop catches this and retries with the value stringified.

```mermaid
flowchart TD
    A[Start: Loop over published components] --> B[Build document with spec]
    B --> C[client.update to ES]

    C -->|Success| D[indexing_success += 1]
    D --> A

    C -->|BadRequestError| E{_parse_text_conflict_field:<br/>Is it a document_parsing_exception<br/>for a text field?}

    E -->|Yes, e.g. spec.inputs.type| F[Add path to text_field_paths set]
    F --> G[_sanitize_spec_with_mapping:<br/>stringify dict/list values<br/>at known text paths]
    G --> H[Retry client.update with<br/>sanitized document]

    H -->|Success| I[indexing_success += 1]
    I --> A

    H -->|Failure| J[indexing_errors += 1<br/>log: Retry failed]
    J --> A

    E -->|No, different error| K[indexing_errors += 1<br/>log: ES BadRequestError]
    K --> A

    C -->|Other Exception| L[indexing_errors += 1<br/>log: Unexpected error]
    L --> A
```

## Key Points

- **No pre-sanitization** -- the raw spec goes straight to ES
- **Single sanitization point** -- only inside the `except BadRequestError` path
- `text_field_paths` accumulates across the loop, so if `spec.inputs.type` conflicts on doc B, it's in the set for docs C and D -- but they still go through the same error → sanitize → retry flow (no shortcut)

## 0.6.0 (2025-08-07)

### Feat

- Add proof-of-concept for transaction management (#39)
- Add update page functionality to the Notion clients (#46)

### Fix

- Add standard module header with copyright and license notes.

## 0.5.0 (2025-08-05)

### Feat

- Add initial implementation proof-of-concept transaction management
- Add fully DBAPI 2.0 compliant cursor description

### Fix

- Fix non compliance with DBAPI 2.0 (#41)
- Fix cross-reference issue with fully qualified names (#38).

### Refactor

- Adapt `CursorResult` and `Row` (#40).

## 0.4.0 (2025-07-27)

### Feat

- Provide a URI schema for `normlite` (#24)
- Add `WHERE` clause to AST (#13)
- Add SqlAlchemy-like `text()` construct.
- Add ability to compile SQL nodes.

### Refactor

- Repurpose the Notion client.

## 0.3.0 (2025-07-24)

### Feat

- Add DBAPI2 `Cursor` full implementation.
- Add parameter binding to operation execution (#15)
- Extend DBAPI2.0 implementation and Notion SDK functionality.
- Add `execute()` method to `Cursor` for the `pages.create` endpoint.
- Add fetching data capabilities (1)
- Add semantic logic for Notion databases and pages.
- Add fetching data capabilities

### Fix

- Render example code in `notiondbapi.Cursor`
- Add check for Notion JSON schema

### Refactor

- Redesign parsing for Notion objects.
- Refactor `Engine.__init__()` method.
- Refactor `notions_sdk.py` module

## 0.2.0 (2025-07-12)

### Feat

- Add SQL to JSON cross-compilation.
- Add SQL module for SQL parsing
- Add create engine

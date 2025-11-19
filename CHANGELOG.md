## 0.7.0 (2025-11-19)

### Feat

- add random access to `InMemoryNotionClient` store (#90).
- add random access to `InMemoryNotionClient` store.
- add random access to `InMemoryNotionClient` store.
- add random access to `InMemoryNotionClient` store.
- add random access to `InMemoryNotionClient` store.
- add random access to `InMemoryNotionClient` store.
- Add engine abstraction and connection management (#77)
- Add an SQL compiler infrastructure (#100)
- Cursor result and row management #99
- Provide programmatic API for creating and reflecting tables (#97)
- Add DBAPI 2.0 compliant Connection class (#96)
- Extend the Notion row and description visitors to support property identifiers (#91).
- Extend the class `InMemoryNotionClient` to provide property ids (#89).
- Add full proxy server functionality for executing INSERT.
- Add a client for interacting with the proxy server (#73).
- Add `CREATE TABLE` SQL statement execution (#30)
- Add central registry of schema objects feature to MetaData (#68)
- Add ``autoload_with`` keyword option to reflect a table in its constructor (#66).
- Add table creation flow (#62)
- Add information_schema management to InMemoryNotionClient (#63)
- Add the `Table` SQL construct (#54).
- Add Notion type system (#53)
- Add Notion type system
- Add Notion type system
- Explore code generator for SQL node classes
- Initial commit for new SQL compiler features.
- Add fully DBAPI compliant `Connection` and `Cursor` classes (#50)
- Add DBAPI 2.0 compliant Connection (#48).

### Fix

- `_parser.parse_property() does not handle retrieved objects correctly
- `InMemoryNotionClient._add() does not validate payload correctly (#93)
- Fix issues with Notion specific database properties (#49).
- String bind processor delivers wrong Notion object (#87)
- Parsing text content does not handle correctly lists of text objects (#86)
- Do not provide the key "type" to get_col_spec() in the type engine system (#85)
- Fix error handling in transactions and operations.

### Refactor

- InMemoryNotionClient to always contain an initial root page (#103).
- Add execution context
- Refactor SQL `INSERT` statement (#55)
- Extend `CursorResult` to be compatible with single and multiple result sets (#51)

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

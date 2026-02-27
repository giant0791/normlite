## 0.8.0 (2026-02-27)

### Feat

- **engine**: Add `ReflectTable` construct (WIP).
- **engine**: Add `DropTable` as DDL statement ([#177](https://github.com/giant0791/normlite/issues/177)).
- **notion_sdk**: add `search` endpoint ([#180](https://github.com/giant0791/normlite/issues/180)).
- **notion_sdk**: add `search` endpoint (WIP).
- **sql**: Add `DropTable` DDL statement.
- **notion_sdk**: Add `databases_update()` to the in-memory Notion client.
- **sql**: Add `DropTable` DDL statement.
- **engine**: Add table "tables" update and exists capabilities in `Engine` ([#173(https://github.com/giant0791/normlite/issues/173)).
- **engine**: Add `Engine` helpers to manage system "tables".
- **engine**: Extend `Connection.execute()` behavior when parameters are provided ([#134](https://github.com/giant0791/normlite/issues/134)).
- **util**: Add normlite deprecated decorator as utility.
- **main**: Add `frozendict` class for immutable dictionaries.
- **sql**: Add `SELECT` DML statement ([#140](https://github.com/giant0791/normlite/issues/140)).
- **sql**: Add column projection to `Select` statement.
- **sql**: Add SlqAlchemy-like function operators.
- **sql**: Add safety guard by forbidding Python truthiness test on column expressions.
- **sql**: Add `startswith` operator.
- **sql**: Add method to return user-defined columns only.
- **sql**: Add support for after and before date operators.
- **sql**: Add compilation of unary expressions.
- **sql**: add `WHERE` clause to `Select` construct.
- **sql**: add `Select` DML construct (no `WHERE` clause).
- **sql**: Add `not_in`, `endswith` for `String` and `before` for `Date`.
- **sql**: add operators "!=" and "in_" in  binary expressions with String columns.
- **sql**: add column binary expressions [#147](https://github.com/giant0791/normlite/issues/151)
- **sql**: add SQL AST elements for column expressions.
- **sql**: Reflect all tables in `MetaData` [#142](https://github.com/giant0791/normlite/issues/142)
- **sql**: Add ReflectTable as DDL executable [#125](https://github.com/giant0791/normlite/issues/125)
- add `HasTable` as DDL executable ([119](https://github.com/giant0791/normlite/issues/119)).

### Fix

- Reverse misspelling of `Connection._execution_options` attribute.
- **engine**: Replace result processor with getter in `ReflectedTableInfo.from_dict()`.
- **sql**: `Boolean` result process shall handle dictionaries too.
- **engine**: Remove unused variable in `Row._process_dml_row()`.
- **sql**: Normalize INSERT/SELECT compilation of resulting columns.
- **sql**: `visit_select()` misspells endpoint
- **notion_sdk**: Remove duplicate typing imports.
- **sql**: Modify `get_col_spec()` method to return the actual number format.
- Multiple issues in the type API.
- Outdated imports for `CursorResult` and `Row`.
- Remove dialect parameter from type engine API [#124](https://github.com/giant0791/normlite/issues/124)
- Proper bind processing for bind parameter [#146](https://github.com/giant0791/normlite/issues/146)
- **notiondbapi**: Refactor DescriptionCompiler to return metadata columns only [#136](https://github.com/giant0791/normlite/issues/136).

### Refactor

- **normlite**: Add flat imports for more ergonomic library usage ([#186](https://github.com/giant0791/normlite/issues/186)).
- **engine**: Refactor `Inspector.has_table()` method to reflect the new table lifecycle states ([#184](https://github.com/giant0791/normlite/issues/184)).
- **sql**: Refactor `ReflectTable` for new execution pipeline ([#166](https://github.com/giant0791/normlite/issues/166)).
- **engine**: Complete the migration of `Engine` table metadata handling to `SystemCatalog`.
- **engine**: Add public API for `SystemCatalog`.
- **engine**: Move system catalog `Engine` methods to specialized class (WIP).
- **sql**: Major refactoring of `Table.create()` / `Table.drop()` (WIP)
- **sql**: Major refactoring of  (WIP).
- **sql**: Add write-only cache for system tables page id in `Table`.
- **engine**: Refactor `HasTable` for new execution pipeline ([#164](https://github.com/giant0791/normlite/issues/164)).
- **sql**: Add comprehensive argument validation to `Table`.
- **sql**: Remove obsolete code.
- **sql**: Remove obsolete `SQLCompiler.visit_create_column()`.
- **sql**: Refactor `Table.create()` ([#175](https://github.com/giant0791/normlite/issues/175)).
- **sql**: Implement `checkif` logic in `Table.create()`.
- **engine**: Refactor `CreateTable` for new execution pipeline ([#165](https://github.com/giant0791/normlite/issues/165)).
- **sql**: Update compilation for `CreateTable` to new `AbstractNotionClient` API.
- **engine**: Refactor statement execution pipeline ([#158](https://github.com/giant0791/normlite/issues/158)).
- **engine**: Harmonize page/database structure across `Engine` and `InMemoryNotionClient` ([#160](https://github.com/giant0791/normlite/issues/160))
- **notion_sdk**: Add normalization layer to `InMemoryNotionClient` ([#161](https://github.com/giant0791/normlite/issues/161)).
- **notion_sdk**: Refactor `InMemoryNotionClient` class ([159](https://github.com/giant0791/normlite/issues/159)).
- **notion_sdk**: Start of refactor for the in-memory Notion client.
- **engine**: Refactor `ExecutionContext`.
- **sql**: Refactor `NotionCompiler` to handle bind parameters.
- Insert statement compilation.
- **sql**: Refactor operators management.
- **sql**: Refactor all DML elemets classes.
- **sql**: Refactor parameter handling in compilation.
- **sql**: Refactor new parameter handling in the compilation.
- **notion_sdk**: Refactor parameter handling in `AbstractNotionClient` and  `InMemoryClient`.
- **sql**: Refactor column element hierarchy.
- **sql**: Refactor value binding for `Insert` DML constructs.
- **sql**: Refactor autoload behavior in `Table` [#139](https://github.com/giant0791/normlite/issues/139)
- **notiondbapi**: refactor Notion models and parsing [#135](https://github.com/giant0791/normlite/issues/135)

## 0.7.0 (2025-11-19)

### Feat

- add random access to `InMemoryNotionClient` store ([#90](https://github.com/giant0791/normlite/issues/90)).
- Add engine abstraction and connection management ([#77](https://github.com/giant0791/normlite/issues/77))
- Add an SQL compiler infrastructure ([#100](https://github.com/giant0791/normlite/issues/100))
- Cursor result and row management ([#99](https://github.com/giant0791/normlite/issues/99))
- Provide programmatic API for creating and reflecting tables ([#97](https://github.com/giant0791/normlite/issues/97))
- Add DBAPI 2.0 compliant Connection class ([#96](https://github.com/giant0791/normlite/issues/96))
- Extend the Notion row and description visitors to support property identifiers ([#91](https://github.com/giant0791/normlite/issues/91)).
- Extend the class `InMemoryNotionClient` to provide property ids ([#89](https://github.com/giant0791/normlite/issues/89)).
- Add full proxy server functionality for executing INSERT.
- Add a client for interacting with the proxy server ([#73](https://github.com/giant0791/normlite/issues/73)).
- Add `CREATE TABLE` SQL statement execution ([#30](https://github.com/giant0791/normlite/issues/30))
- Add central registry of schema objects feature to MetaData ([#68](https://github.com/giant0791/normlite/issues/68))
- Add ``autoload_with`` keyword option to reflect a table in its constructor ([#66](https://github.com/giant0791/normlite/issues/66)).
- Add table creation flow ([#62](https://github.com/giant0791/normlite/issues/62))
- Add information_schema management to InMemoryNotionClient ([#63](https://github.com/giant0791/normlite/issues/63))
- Add the `Table` SQL construct ([#54](https://github.com/giant0791/normlite/issues/54)).
- Add Notion type system ([#53](https://github.com/giant0791/normlite/issues/53))
- Explore code generator for SQL node classes
- Initial commit for new SQL compiler features.
- Add fully DBAPI compliant `Connection` and `Cursor` classes ([#50](https://github.com/giant0791/normlite/issues/50))
- Add DBAPI 2.0 compliant Connection ([#48](https://github.com/giant0791/normlite/issues/48)).

### Fix

- `_parser.parse_property()` does not handle retrieved objects correctly
- `InMemoryNotionClient._add()` does not validate payload correctly ([#93](https://github.com/giant0791/normlite/issues/93))
- Fix issues with Notion specific database properties ([#49](https://github.com/giant0791/normlite/issues/49)).
- String bind processor delivers wrong Notion object ([#87](https://github.com/giant0791/normlite/issues/87))
- Parsing text content does not handle correctly lists of text objects ([#86](https://github.com/giant0791/normlite/issues/86))
- Do not provide the key "type" to get_col_spec() in the type engine system ([#85](https://github.com/giant0791/normlite/issues/89))
- Fix error handling in transactions and operations.

### Refactor

- InMemoryNotionClient to always contain an initial root page ([#103](https://github.com/giant0791/normlite/issues/103)).
- Add execution context
- Refactor SQL `INSERT` statement ([#55](https://github.com/giant0791/normlite/issues/55))
- Extend `CursorResult` to be compatible with single and multiple result sets ([#51](https://github.com/giant0791/normlite/issues/51))

## 0.6.0 (2025-08-07)

### Feat

- Add proof-of-concept for transaction management ([#39](https://github.com/giant0791/normlite/issues/39))
- Add update page functionality to the Notion clients ([#46](https://github.com/giant0791/normlite/issues/46))

### Fix

- Add standard module header with copyright and license notes.

## 0.5.0 (2025-08-05)

### Feat

- Add initial implementation proof-of-concept transaction management
- Add fully DBAPI 2.0 compliant cursor description

### Fix

- Fix non compliance with DBAPI 2.0 ([#41](https://github.com/giant0791/normlite/issues/41))
- Fix cross-reference issue with fully qualified names ([#38](https://github.com/giant0791/normlite/issues/38)).

### Refactor

- Adapt `CursorResult` and `Row` ([#40](https://github.com/giant0791/normlite/issues/40)).

## 0.4.0 (2025-07-27)

### Feat

- Provide a URI schema for `normlite` ([#24](https://github.com/giant0791/normlite/issues/24))
- Add `WHERE` clause to AST ([#13](https://github.com/giant0791/normlite/issues/13))
- Add SqlAlchemy-like `text()` construct.
- Add ability to compile SQL nodes.

### Refactor

- Repurpose the Notion client.

## 0.3.0 (2025-07-24)

### Feat

- Add DBAPI2 `Cursor` full implementation.
- Add parameter binding to operation execution ([#15](https://github.com/giant0791/normlite/issues/15))
- Extend DBAPI2.0 implementation and Notion SDK functionality.
- Add `execute()` method to `Cursor` for the `pages.create` endpoint.
- Add fetching data capabilities
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

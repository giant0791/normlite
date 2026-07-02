# ADR-0013: CheckConstraint — catalog persistence and the JSON-AST reflection contract

**Status:** Accepted
**Date:** 2026-07-01

---

## Context

A `CheckConstraint` has no Notion-database backend
([ADR-0012](./0012-checkconstraint-client-side-enforcement.md)): it cannot be written into the
Notion database schema, so a naive design would **lose all checks on reflection**
(`Table(autoload_with=engine)` would rebuild a table with no constraints — an invisible
asymmetry between a code-defined table and the same table reflected).

normlite already keeps its own `information_schema` catalog — a Notion `tables` database under an
`information_schema` page, carrying a row per user table (`table_name`, `table_catalog`,
`table_schema`, `table_id`). That catalog is normlite's, not Notion's, so it *can* hold data
Notion's database schema cannot.

## Decision

**Persist check definitions in normlite's own catalog and reflect them back, storing the
predicate as a versioned JSON AST.**

- **A dedicated `check_constraints` catalog database** under `information_schema`, sibling of
  `tables`, mirroring SQL `information_schema.check_constraints`. Bootstrapped with one more
  `_get_or_create_database` call. Rows carry `constraint_name` (title), `table_catalog` +
  `table_name` (owning table), and `check_clause`. `CreateTable`/`create_all` writes the rows
  after `databases.create`; `DropTable` removes/marks them; reflection reads them back and
  **rebuilds `CheckConstraint` objects, binding column refs to the reflected table's columns by
  name.**
- **`check_clause` is a versioned JSON AST**, not a textual `"price > 0"` clause. normlite has
  no SQL-text parser (a col-col comparison is already inexpressible as Notion text), so a JSON
  AST round-trips without building one. Root field `"v": 1` makes the format a data contract
  that can evolve without silently misreading old rows. Node kinds:
  - `compare` — `{"kind":"compare","op":"<Operator enum name>","left":{col},"right":{col|lit}}`.
    `op` is the `Operator` **enum name** (`"GT"`, `"LT"`, `"EQ"`, … — backend-agnostic, stable
    across renames of Python operators). `left` is **always** a `col`; `right` is a `col`
    (col-col) or a `lit`.
  - `bool` — `{"kind":"bool","op":"and|or","clauses":[…]}` (`BooleanClauseList`).
  - `not`  — `{"kind":"not","operand":{…}}` (`UnaryExpression`).
  - `col`  — `{"kind":"col","name":"price"}`; rebound to the reflected table's column by name.
  - `lit`  — value stored as a **string**, **re-coerced through the paired column's
    `TypeEngine`** on deserialize (`Decimal("0")` via `price.type_`). This dodges JSON
    float-precision loss and needs no per-literal type tag, because every literal is paired with
    a known column in its `compare` node.

## Considered Options

- **Do not persist; accept that reflection loses checks.** Rejected here (this ADR is the
  reversal of that boundary): the code-vs-reflected asymmetry is a real footgun, and the catalog
  is the natural place to close it.
- **Store checks as a JSON blob column on each `tables` row.** Rejected: denormalized, unlike
  SQL's own catalog, and awkward once other constraint kinds (NOT NULL, UNIQUE) arrive. A
  dedicated `check_constraints` database is the SQL-faithful, extensible home.
- **Store `check_clause` as textual SQL (`"price > 0"`).** Rejected: human-readable but requires
  a parser to rebuild, which normlite deliberately does not have; and col-col predicates have no
  natural Notion-textual form. (A human-readable rendering may be stored *alongside* the JSON in
  a later revision, but the JSON AST is canonical.)
- **Tag every literal with its own type.** Rejected as redundant: each literal is paired with a
  known column, so the column's `TypeEngine` reconstructs it.

## Consequences

- The catalog bootstrap and drop paths grow a second constraint-bearing database; `create_all`
  and `DropTable` must keep `check_constraints` rows in sync with table lifecycle.
- `check_clause` is now a **persisted, versioned data contract**. Any format change must bump
  `"v"` and keep a reader for old versions — old rows written to a user's Notion workspace must
  stay readable.
- Reflection rebinds `col` nodes **by name**; a stored check referencing a column that no longer
  exists in the reflected table is an error surface to be handled when slice 4 lands.
- This is delivered as its own slices (serializer + catalog write, then reflection deserialize)
  after the [ADR-0012](./0012-checkconstraint-client-side-enforcement.md) enforcement tracer
  bullet — a table defined and reflected before those slices land loses its checks, which is
  acceptable for the in-memory define-then-use path slices 1–2 target.

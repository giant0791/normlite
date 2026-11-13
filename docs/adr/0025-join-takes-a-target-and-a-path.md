# ADR-0025: join() takes a target and a path

**Status:** Accepted
**Date:** 2026-11-13
**Issue:** #480

## Context

Before 0.14.0, `Select.join(onclause)` and `Select.outerjoin(onclause)` took one argument with two
meanings:

- A `Column` or a `BinaryExpression` named the **path**, that is, the `Relation` column on the left
  table.
- A `Table` named the **destination**, that is, the right table.

A user had to know this type rule to call `join()` correctly. The two forms also behaved
differently: `select(func.count()).join(students.c.enrolled_in)` found its left table, but
`select(func.count()).join(courses)` needed `select_from(students)`.

normlite also accepted `join(students.c.enrolled_in.contains(u))`, but it kept only the column. The
`contains(u)` predicate disappeared with no error.

## Decision

1. **The signature is `join(target, onclause=None)`.** `outerjoin()` has the same signature. Each
   argument names one side of the join. `target` is always the right table, and it must be a
   `Table`. This aligns normlite with SQLAlchemy Core.
2. **The onclause is a path, not a condition.** The onclause is the `Relation` column on the left
   table that references the target. The join condition is always implicit: the left `Relation`
   cell contains the right `object_id`. With no onclause, normlite searches the foreign keys of the
   left table for the `Relation` column that references the target.
3. **`join()` rejects a `BinaryExpression` onclause** with an `ArgumentError`. The message tells the
   user to pass the `Relation` column and to move the predicate to `where()`.
4. **`join()` does not find the left table from the target.** If the SELECT has no left table and
   no onclause, `join()` raises an `ArgumentError`. The remedy is `select_from()` or an onclause.

All three errors are `ArgumentError`s, because the `join()` call alone proves them. `join()` builds
the `Join` node immediately, so a later `select_from()` cannot repair it.

## Reasons

- **Decision 1:** one argument with two meanings is hard to learn and easy to get wrong. SQLAlchemy
  Core uses `join(target, onclause)`, so normlite users who know Core already know the shape.
- **Decision 2:** normlite cannot express a SQL `ON` condition. A column-to-column comparison such
  as `students.c.enrolled_in == courses.c.object_id` does not build. The code already used only the
  column.
- **Decision 3:** moving the predicate to the WHERE is correct for `join()` but wrong for
  `outerjoin()`. An ON predicate on an outer join keeps each left row and gives it a NULL right
  side. A WHERE predicate drops that left row. So the move replaces one silent wrong answer with
  another. `where()` already gives the inner-join behavior, so the rejection loses no capability.
- **Decision 4:** a scan of `target.metadata` for the tables that reference the target is easy to
  write, but its answer is not safe:
  - Two tables can reference the same target, and nothing in the statement chooses between them.
  - A new table in the MetaData can change the answer of a statement that never names that table.
  - SQLAlchemy Core does not infer the left table either. It asks for `select_from()`.

## Considered options

- **Keep the one-argument API and document the type rule** (option 1 in #480). This keeps the
  ambiguity.
- **A separate parameter or method for the path form** (option 3 in #480). This adds API surface,
  and it does not match SQLAlchemy.
- **ON-predicate semantics for a `BinaryExpression` onclause.** This needs the NULL behavior of
  ADR-0019 for a new case. It is a feature, not part of an API refactor.

## Consequences

- This is a public API break in 0.14.0. The 0.13 call `join(students.c.enrolled_in)` now raises
  `join() target must be a Table, got Column`. The migration is `join(courses,
  students.c.enrolled_in)`, or `join(courses)` when the left table has only one `Relation` column
  to `courses`.
- An onclause predicate moves to `where()`: `join(courses, col.contains(u))` becomes
  `join(courses, col).where(col.contains(u))`.
- If normlite later needs ON-predicate semantics, it needs a new argument or construct. The
  onclause stays a path.

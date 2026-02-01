Below is a **production-quality `tests/README.md`** aligned with your actual architecture, terminology, and testing goals.

You can copy this verbatim.

---

# `tests/` — Test Architecture & Philosophy

This directory contains **all tests and test-only infrastructure** for `normlite`.

The guiding principle is that **production code lives exclusively in `src/`**, while `tests/` contains:

* reference implementations
* generators and utilities
* axiomatic, differential, and round-trip tests

Nothing under `tests/` is imported by production code.

---

## 1. Test philosophy & validation pipeline

`normlite` is validated using **three complementary testing strategies**:

### 1.1 Axiomatic testing (local correctness)

These tests validate **fundamental invariants** (“axioms”) of a component in isolation.

Examples:

* Boolean logic identities
* AST structural invariants
* Compiler output shape constraints

Purpose:

* Catch local regressions early
* Make intent explicit

---

### 1.2 Differential testing (semantic equivalence)

These tests compare **two independent implementations of the same semantics**:

* **Reference implementation** (simple, explicit, slow, trusted)
* **Production implementation** (optimized, complex)

Examples:

* Reference evaluator vs production evaluator
* Reference compiler vs production compiler

Purpose:

* Detect subtle semantic mismatches
* Avoid oracle problems

---

### 1.3 Round-trip testing (structural stability)

These tests validate **lossless transformations** across representations.

Canonical pipeline:

```
AST (randomly generated)
   ↓
PythonExpressionCompiler
   ↓
Python source code (string)
   ↓
exec()
   ↓
AST (reconstructed)
```

The test asserts:

```
ast_equal(original_ast, reconstructed_ast) == True
```

Purpose:

* Guarantee compiler correctness
* Detect hidden asymmetries or normalization bugs

---

## 2. Directory & file structure

```text
tests/
├── conftest.py
│   Shared pytest fixtures (tables, metadata, RNG seeds, helpers)
│
├── support/
│   Test-only infrastructure and utilities
│
│   ├── generators.py
│   │   ASTGenerator, ExpressionGenerator, EntityRandomGenerator
│   │
│   ├── ast_equal.py
│   │   Structural AST equality (used by round-trip tests)
│   │
│   ├── exec_utils.py
│   │   Safe exec helpers for PythonExpressionCompiler
│   │
│   └── coverage.py
│       Optional operator/type coverage helpers
│
├── reference/
│   Reference semantics (non-production, trusted baseline)
│
│   ├── evaluator.py
│   │   Reference evaluator for Notion-like queries
│   │
│   ├── compiler.py
│   │   Reference AST → JSON compiler
│   │
│   └── test_axioms.py
│       Axioms validating reference behavior
│
├── ast/
│   AST-level tests (implementation-independent)
│
│   └── test_ast_axioms.py
│
├── compiler/
│   Compiler-focused tests
│
│   ├── test_compiler_axioms.py
│   │   Laws of the reference compiler
│   │
│   ├── test_compiler_differential.py
│   │   Reference compiler vs production compiler
│   │
│   ├── test_pycompiler.py
│   │   AST → PythonExpressionCompiler unit tests
│   │
│   └── test_pycompiler_roundtrip.py
│       AST ↔ Python round-trip tests
│
├── evaluator/
│   Evaluator tests
│
│   └── test_differential.py
│       Reference evaluator vs production evaluator
│
├── integration/
│   Full pipeline tests
│
│   └── test_pipeline.py
│
└── staging/
    Deprecated or quarantined code kept for audit
```

---

## 3. How to run tests

### Run everything

```bash
pytest
```

---

### Run a specific category

AST tests:

```bash
pytest tests/ast
```

Compiler tests:

```bash
pytest tests/compiler
```

Evaluator tests:

```bash
pytest tests/evaluator
```

Round-trip tests only:

```bash
pytest tests/compiler/test_pycompiler_roundtrip.py
```

---

### Run slow / fuzz-style tests only (recommended in CI nightly)

If markers are enabled:

```bash
pytest -m slow
```

---

### Deterministic runs

All generators accept a seed.

Example (via fixture or env):

```bash
PYTHONHASHSEED=0 pytest
```

---

## 4. Extending the tests

### 4.1 Adding a new `TypeEngine`

When introducing a new `TypeEngine`:

1. **Production**

   * Implement the type in `src/normlite/sql/types.py`
   * Ensure all relevant `ColumnOperators` are supported

2. **Generators**

   * Update `ASTGenerator._gen_value()`
   * Update `ExpressionGenerator._emit_binary()`
   * Ensure random value generation is deterministic and serializable

3. **Reference evaluator**

   * Add evaluation semantics in `tests/reference/evaluator.py`

4. **Reference compiler (if applicable)**

   * Add JSON compilation logic in `tests/reference/compiler.py`

5. **Tests**

   * Add axioms (if applicable)
   * Extend round-trip tests automatically via generators
   * Add at least one targeted differential test

---

### 4.2 Adding a new `ColumnOperator`

When introducing a new operator:

1. **Production**

   * Implement operator in `ColumnOperators`
   * Ensure evaluator and compiler support it

2. **Generators**

   * Add operator to the operator pool
   * Ensure both AST and expression generators emit it

3. **AST equality**

   * Ensure `ast_equal()` understands the operator

4. **Tests**

   * Differential tests will automatically exercise it
   * Add one explicit axiom if the operator has algebraic laws

---

## 5. Design principles

* **No production code in `tests/`**
* **Reference code is simple, explicit, and trusted**
* **Generators are infrastructure, not tests**
* **Round-trip tests start from AST, never from strings**
* **Every bug fix should add a test**

---

## 6. When in doubt

Ask:

> “Can this fail silently without a reference or round-trip test?”

If the answer is yes, add a test.

---

**This test suite is designed to scale with the language.
If a change breaks semantics, it should fail loudly and early.**

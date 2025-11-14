# tests/factories/generators.py
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# ---------------------------------------------------------------------
# EXPERIMENTAL CODE, DON'T USE!!!
# ---------------------------------------------------------------------


import random
import uuid
from decimal import Decimal
from faker import Faker

# normlite imports (adjust paths if needed)
from normlite.sql.schema import Table, Column
from normlite.sql.type_api import (
    String,
    Integer,
    Numeric,
    Money,
    Boolean,
    Date,
)

class RandomTableGenerator:
    """
    Random test data generator for normlite Notion-like tables.

    Responsibilities:
    - Generate Table objects with random columns.
    - Ensure exactly 1 title column (String(is_title=True)).
    - Generate realistic data using Faker.
    - Produce Notion-compatible page dictionaries.
    """

    def __init__(self, faker: Faker = None, seed: int | None = None):
        self.faker = faker or Faker()
        if seed is not None:
            random.seed(seed)
            self.faker.seed_instance(seed)

        # Domain-based profiles for title generation
        self.domain_title_generators = [
            self.faker.name,
            self.faker.catch_phrase,
            self.faker.bs,
            self.faker.job,
            self.faker.company,
            self.faker.sentence,
        ]

    # ---------------------------------------------------------------------
    # Column → faker adapters
    # ---------------------------------------------------------------------
    def generate_value_for_column(self, col: Column):
        col_type = col.type_

        if isinstance(col_type, String):
            if col_type.is_title:
                # realistic domain-profile title
                return random.choice(self.domain_title_generators)()
            else:
                return self.faker.word()

        if isinstance(col_type, Integer):
            return self.faker.pyint(min_value=0, max_value=10_000)

        if isinstance(col_type, Numeric):
            return Decimal(str(self.faker.pydecimal(left_digits=5, right_digits=2)))

        if isinstance(col_type, Money):
            amount = Decimal(str(self.faker.pydecimal(left_digits=4, right_digits=2)))
            return amount

        if isinstance(col_type, Boolean):
            return self.faker.pybool()

        if isinstance(col_type, Date):
            return self.faker.date_between(start_date="-5y", end_date="today")

        raise TypeError(f"Unsupported column type: {type(col_type)}")

    # ---------------------------------------------------------------------
    # Table Generator
    # ---------------------------------------------------------------------
    SUPPORTED_TYPES = [Integer, Numeric, Money, Boolean, Date, String]

    def random_column(self, idx: int, *, is_title=False) -> Column:
        """
        Creates a random Column instance.
        Exactly one title column is created using is_title=True.
        """
        name = f"col_{idx}"

        if is_title:
            return Column(name, String(is_title=True))

        # choose a random type excluding title-String
        type_cls = random.choice(self.SUPPORTED_TYPES)

        if type_cls is String:
            return Column(name, String(is_title=False))

        if type_cls is Money:
            # pick random currency — expand if normlite defines more
            from normlite.sql.type_api import Currency
            currency = random.choice(list(Currency))
            return Column(name, Money(currency))

        return Column(name, type_cls())

    def generate_random_table(
        self,
        metadata,
        name: str | None = None,
        min_cols: int = 3,
        max_cols: int = 8,
    ) -> Table:
        """
        Creates a Table with random columns but ensures exactly one title column.
        """
        name = name or f"table_{uuid.uuid4().hex[:6]}"
        num_columns = random.randint(min_cols, max_cols)

        # ensure exactly one title column
        title_col = self.random_column(0, is_title=True)

        other_cols = [
            self.random_column(i + 1, is_title=False)
            for i in range(num_columns - 1)
        ]

        return Table(name, metadata, *( [title_col] + other_cols ))

    # ---------------------------------------------------------------------
    # Page generator
    # ---------------------------------------------------------------------
    def generate_page(self, table: Table) -> dict:
        """
        Creates a single Notion-compatible page JSON object based on table schema.
        """
        props = {}
        for col in table.columns.values():
            value = self.generate_value_for_column(col)
            json_val = self.compile_property_json(col, value)
            props[col.name] = json_val

        parent = {'type': 'database_id', 'database_id': uuid.uuid4()}

        return {
            "parent": parent,
            "properties": props,
        }

    def compile_property_json(self, col: Column, value):
        """
        Converts Python values into Notion API JSON structure.
        Uses the NotionSQLCompiler to preserve schema rules.
        """
        bind_proc = col.type_.bind_processor
        return bind_proc(value)

    # ---------------------------------------------------------------------
    # Bulk page generation
    # ---------------------------------------------------------------------
    def generate_pages(self, table: Table, count: int = 10) -> list[dict]:
        return [self.generate_page(table) for _ in range(count)]
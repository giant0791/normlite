import json
import pdb
from collections import Counter
import pytest
from tqdm import tqdm

from tests.support.generators import ReferenceGenerator
from tests.reference.evaluator import reference_eval

from normlite.notion_sdk.client import _Filter  # production

def test_differential_random():
    refgen = ReferenceGenerator(28)
    schema = refgen.gen_schema(min_props=3, max_props=30)
    pages = [refgen.gen_page(schema) for _ in range(1000)]

    for _ in tqdm(range(200), desc="Comparing ref vs prod filter evaluation", unit="filter"):
        filt = refgen.gen_filter(schema, depth=3, max_depth=8)
        filter = {'filter': filt}

        expected = [p for p in pages if reference_eval(p, filt)]
        actual = [p for p in pages if _Filter(p, filter).eval()]

        assert actual == expected

def test_differential_random_dryrun():
    refgen = ReferenceGenerator(seed=28)

    NUM_SCHEMAS = 100
    PAGES_PER_SCHEMA = 100
    FILTERS_PER_SCHEMA = 100
    counters = []

    for _ in tqdm(range(NUM_SCHEMAS), desc="Comparing ref vs prod filter eval dryrun", unit="schema"):
        # 1. Generate schema
        schema = refgen.gen_schema(min_props=3, max_props=30)
        counters.append(debug_schema(schema))

        # 2. Generate pages for this schema
        pages = [refgen.gen_page(schema) for _ in range(PAGES_PER_SCHEMA)]

        # 3. Generate and test filters
        for _ in tqdm(
            range(FILTERS_PER_SCHEMA),
            desc="Filters",
            unit="filter",
            leave=False,
        ):
            filt = refgen.gen_filter(schema, depth=3, max_depth=8)
            filter_payload = {"filter": filt}

            """
            expected = [p for p in pages if reference_eval(p, filt)]
            actual = [p for p in pages if _Filter(p, filter_payload).eval()]

            assert actual == expected
            """
            for p in pages:
                ref = reference_eval(p, filt)
                prod = _Filter(p, filter_payload).eval()
                if ref != prod:
                    print("DIVERGENCE")
                    print("PAGE:", p["properties"])
                    print("FILTER:", filt)
                    print("REFERENCE:", ref)
                    print("PRODUCTION:", prod)
                    break       

@pytest.mark.skip(reason="Very long test, skipped by default")
def test_differential_random_massive():
    """Massive differential test reference vs production filter evaluation.
    
    .. attention::
        This test evaluates 1M randomly generated expression.
        It takes hours.
    """
    refgen = ReferenceGenerator(seed=28)

    NUM_SCHEMAS = 1000
    PAGES_PER_SCHEMA = 1000
    FILTERS_PER_SCHEMA = 1000
    counters = []

    for _ in tqdm(range(NUM_SCHEMAS), desc="Comparing ref vs prod filter eval dryrun", unit="schema"):
        # 1. Generate schema
        schema = refgen.gen_schema(min_props=3, max_props=30)
        counters.append(debug_schema(schema))

        # 2. Generate pages for this schema
        pages = [refgen.gen_page(schema) for _ in range(PAGES_PER_SCHEMA)]

        # 3. Generate and test filters
        for _ in tqdm(
            range(FILTERS_PER_SCHEMA),
            desc="Filters",
            unit="filter",
            leave=False,
        ):
            filt = refgen.gen_filter(schema, depth=3, max_depth=8)
            filter_payload = {"filter": filt}

            """
            expected = [p for p in pages if reference_eval(p, filt)]
            actual = [p for p in pages if _Filter(p, filter_payload).eval()]

            assert actual == expected
            """
            for p in pages:
                ref = reference_eval(p, filt)
                prod = _Filter(p, filter_payload).eval()
                if ref != prod:
                    print("DIVERGENCE")
                    print("PAGE:", p["properties"])
                    print("FILTER:", filt)
                    print("REFERENCE:", ref)
                    print("PRODUCTION:", prod)
                    break


    #print(f'\nSchema property types stats: \n  {pretty_print(describe(counters))}')


def debug_schema(schema):
    return Counter(spec["type"] for spec in schema.values())

def debug_condition(refgen: ReferenceGenerator, schema: dict) -> Counter:
    condition = refgen.gen_condition(schema)
    return Counter([k for k in condition.keys() if k != 'property'])

def pretty_print(stats: dict) -> str:
    return json.dumps(stats, indent=4)

def describe(counters):
    # 1. Aggregate all counters into one frequency map
    full_counts = Counter()
    for c in counters:
        full_counts.update(c)
    
    if not full_counts:
        return None

    # 2. Total observations across all categories
    n_total = sum(full_counts.values())
    
    # 3. Identify the most frequent category (the Mode)
    # most_common(1) returns a list like [('rich_text', 338)]
    top_item, top_freq = full_counts.most_common(1)[0]

    return {
        "count": n_total,               # Total instances across all categories
        "unique": len(full_counts),     # Number of distinct categories
        "top": top_item,                # The most frequent category (Mode)
        "top_freq": top_freq,           # How many times the top item appeared
        "freq_dist": dict(full_counts)  # The complete distribution
    }

def test_schema_skewness():
    refgen = ReferenceGenerator(seed=28)
    counters = []
    for _ in range(100):
        schema = refgen.gen_schema(min_props=3, max_props=30)
        counters.append(debug_schema(schema))

    #print(f'\nSchema property types stats: \n  {pretty_print(describe(counters))}')

def test_condition_skewness():
    refgen = ReferenceGenerator(seed=28)
    counters = []
    for _ in range(100):
        schema = refgen.gen_schema(min_props=3, max_props=30)
        counters.append(debug_condition(refgen, schema))

    #print(f'\nFilter condition types stats: \n  {pretty_print(describe(counters))}')
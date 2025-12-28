import pdb
from tests.reference.evaluator import reference_eval
from tests.generators.pages import generate_pages
from tests.generators.filters import generate_filter
from tests.generators.schema import SCHEMA

from normlite.notion_sdk.client import _Filter  # production

def test_differential_random():
    pages = generate_pages(SCHEMA, 100)

    for _ in range(200):
        filt = generate_filter(SCHEMA)
        filter = {'filter': filt}

        expected = [p for p in pages if reference_eval(p, filt)]
        actual = [p for p in pages if _Filter(p, filter).eval()]

        assert actual == expected

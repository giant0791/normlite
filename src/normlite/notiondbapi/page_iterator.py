# notiondbapi/page_iterator.py
# Copyright (C) 2026 Gianmarco Antonini
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



from typing import Optional, Sequence

from normlite.notiondbapi.resultset import ResultSet


class PageIterator:
    _page_fetcher: callable
    _page_size: int
    _start_cursor: str
    _exhausted: bool

    def __init__(self, page_fetcher: callable, page_size: Optional[int]):
        self._page_fetcher = page_fetcher
        self._page_size = page_size or 100
        self._exhausted = False
        self._start_cursor = None

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    def __iter__(self):
        return self
    
    def __next__(self):
        if self._exhausted:
            raise StopIteration
        
        page = self._page_fetcher(self._start_cursor)
        if page.get("has_more", False):
            next_cursor = page.get("next_cursor")
            if next_cursor is None:
                # malformed result object: has_more=True but next_cursor=None
                # set the exhausted flag anyway
                self._exhausted = True
                raise ValueError("Malformed Notion result object")
            
            self._start_cursor = next_cursor
        
        else:
            self._exhausted = True

        return page

class JsonPageSource:
    """Adapt Notion's JSON page pagination to the batch-source contract.

    Notion pages arrive as dicts and the stream ends by raising StopIteration;
    a plan yields decoded tuples and ends by returning None. This is the side
    with the impedance mismatch -- a plan root satisfies :meth:`next` unwrapped.
    """

    def __init__(
        self, 
        page_iter: PageIterator, 
        description: Sequence[tuple], 
        translate: callable,
    ):
        self._page_iter = page_iter
        self._description = description
        self._translate = translate      # Cursor._translate_notion_error

    @property
    def exhausted(self) -> bool:
        return self._page_iter is None or self._page_iter.exhausted

    def next(self):
        if self.exhausted:
            return None
        try:
            obj = next(self._page_iter)
        except StopIteration:
            return None
        except Exception as e:
            _, exc = self._translate(e)   # translation lives HERE, once
            raise exc
        return list(ResultSet.from_json(self._description, obj))


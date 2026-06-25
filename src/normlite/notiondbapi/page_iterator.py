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



class PageIterator:
    _page_fetcher: callable
    _page_size: int
    _start_cursor: str
    _exhausted: bool

    def __init__(self, page_fetcher: callable, page_size=100):
        self._page_fetcher = page_fetcher
        self._page_size = page_size
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

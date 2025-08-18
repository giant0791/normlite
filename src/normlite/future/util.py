from typing import Any, NoReturn


class ReadOnlyCollectionMixin:
    """Mixin for read-only collections."""
    __slots__ = ()

    def _readonly(self) -> NoReturn:
        cls_name = self.__class__.__name__
        raise TypeError(f'{cls_name} object is immutable and/or readonly.')
    
    def __delitem__(self, key: Any) -> NoReturn:
        self._readonly()

    def __setitem__(self, key: Any, value: Any) -> NoReturn:
        self._readonly()

    def __setattr__(self, key: Any, value: Any) -> NoReturn:
        self._readonly()
    